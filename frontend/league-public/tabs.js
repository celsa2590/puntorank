export function showTab(tabId) {
  document.querySelectorAll(".tab-content").forEach(section => {
    section.classList.remove("active");
  });

  document.querySelectorAll(".tab").forEach(button => {
    button.classList.remove("active");
  });

  document.getElementById(tabId).classList.add("active");

  const activeButton = [...document.querySelectorAll(".tab")]
    .find(button => button.getAttribute("onclick")?.includes(tabId));

  if (activeButton) {
    activeButton.classList.add("active");
  }
}
