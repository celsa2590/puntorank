export function initTabs() {
  const tabs = document.querySelectorAll(".tab");
  const contents = document.querySelectorAll(".tab-content");

  function activate(tabId) {
    tabs.forEach(tab => {
      tab.classList.toggle("active", tab.dataset.tab === tabId);
    });

    contents.forEach(content => {
      const isActive = content.id === tabId;
      content.classList.toggle("active", isActive);
      content.style.display = isActive ? "block" : "none";
    });
  }

  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      activate(tab.dataset.tab);
    });
  });

  activate("summary");
}
