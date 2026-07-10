import io
import os
from uuid import uuid4

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from PIL import Image, ImageOps, UnidentifiedImageError


MAX_IMAGE_BYTES = 5 * 1024 * 1024
OUTPUT_SIZE = (512, 512)

ALLOWED_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
}


def get_r2_client():
    account_id = os.getenv("R2_ACCOUNT_ID")
    access_key_id = os.getenv("R2_ACCESS_KEY_ID")
    secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY")

    if not account_id or not access_key_id or not secret_access_key:
        raise RuntimeError(
            "Faltan variables de configuración de Cloudflare R2"
        )

    return boto3.client(
        service_name="s3",
        endpoint_url=(
            f"https://{account_id}.r2.cloudflarestorage.com"
        ),
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )


def process_profile_image(
    raw_image: bytes,
    content_type: str | None,
) -> bytes:
    if not raw_image:
        raise ValueError("La imagen está vacía")

    if len(raw_image) > MAX_IMAGE_BYTES:
        raise ValueError(
            "La imagen supera el tamaño máximo de 5 MB"
        )

    if content_type not in ALLOWED_CONTENT_TYPES:
        raise ValueError(
            "Formato no permitido. Usa JPG, PNG o WEBP"
        )

    try:
        image = Image.open(io.BytesIO(raw_image))
        image.load()
    except (
        UnidentifiedImageError,
        OSError,
        Image.DecompressionBombError,
    ) as exc:
        raise ValueError(
            "El archivo no es una imagen válida"
        ) from exc

    image = ImageOps.exif_transpose(image)
    image = image.convert("RGB")

    image = ImageOps.fit(
        image,
        OUTPUT_SIZE,
        method=Image.Resampling.LANCZOS,
        centering=(0.5, 0.5),
    )

    output = io.BytesIO()

    image.save(
        output,
        format="WEBP",
        quality=82,
        method=6,
        optimize=True,
    )

    return output.getvalue()


def upload_player_photo(
    player_id: int,
    image_bytes: bytes,
) -> tuple[str, str]:
    bucket_name = os.getenv("R2_BUCKET_NAME")
    public_base_url = os.getenv("R2_PUBLIC_BASE_URL")

    if not bucket_name or not public_base_url:
        raise RuntimeError(
            "Faltan R2_BUCKET_NAME o R2_PUBLIC_BASE_URL"
        )

    object_key = (
        f"players/{player_id}/"
        f"avatar-{uuid4().hex}.webp"
    )

    client = get_r2_client()

    try:
        client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=image_bytes,
            ContentType="image/webp",
            CacheControl="public, max-age=31536000, immutable",
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(
            "No se pudo guardar la imagen en Cloudflare R2"
        ) from exc

    photo_url = (
        f"{public_base_url.rstrip('/')}/{object_key}"
    )

    return object_key, photo_url


def delete_player_photo_by_url(
    photo_url: str | None,
) -> None:
    if not photo_url:
        return

    bucket_name = os.getenv("R2_BUCKET_NAME")
    public_base_url = os.getenv(
        "R2_PUBLIC_BASE_URL",
        "",
    ).rstrip("/")

    prefix = f"{public_base_url}/"

    if (
        not bucket_name
        or not public_base_url
        or not photo_url.startswith(prefix)
    ):
        return

    object_key = photo_url[len(prefix):]

    if not object_key.startswith("players/"):
        return

    try:
        get_r2_client().delete_object(
            Bucket=bucket_name,
            Key=object_key,
        )
    except (BotoCoreError, ClientError) as exc:
        print(
            "No se pudo borrar la foto anterior de R2:",
            type(exc).__name__,
            str(exc),
        )
