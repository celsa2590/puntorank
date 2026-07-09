export function initTabs() {
  document.addEventListener("click", (event) => {
    const tab = event.target.closest(".tab");
    if (!tab) return;

    const tabId = tab.dataset.tab;

    document.querySelectorAll(".tab").forEach(t => {
      t.classList.toggle("active", t.dataset.tab === tabId);
    });

    document.querySelectorAll(".tab-content").forEach(section => {
      section.hidden = section.id !== tabId;
    });
  });

  document.querySelectorAll(".tab-content").forEach(section => {
    section.hidden = section.id !== "summary";
  });
}
