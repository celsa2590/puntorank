export function initTabs() {
  const tabs = document.querySelectorAll(".tab");
  const contents = document.querySelectorAll(".tab-content");

  function activate(tabId) {
    tabs.forEach(tab => {
      tab.classList.toggle("active", tab.dataset.tab === tabId);
    });

    contents.forEach(content => {
      content.hidden = content.id !== tabId;
    });
  }

  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      activate(tab.dataset.tab);
    });
  });

  activate("summary");
}
