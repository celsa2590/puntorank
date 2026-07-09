export function initTabs() {

    const tabs = document.querySelectorAll(".tab");

    tabs.forEach(tab => {

        tab.addEventListener("click", () => {

            const tabId = tab.dataset.tab;

            document
                .querySelectorAll(".tab")
                .forEach(t => t.classList.remove("active"));

            document
                .querySelectorAll(".tab-content")
                .forEach(c => c.classList.remove("active"));

            tab.classList.add("active");

            document
                .getElementById(tabId)
                .classList.add("active");

        });

    });

}
