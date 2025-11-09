// Search Enhancements - Loading Indicators

class SearchManager {
    constructor() {
        this.searchInput = null;
        this.spinner = null;
        this.init();
    }

    init() {
        document.addEventListener('DOMContentLoaded', () => {
            this.searchInput = document.getElementById('search-input');
            this.spinner = document.getElementById('search-spinner');

            // Clean up any old search history from localStorage
            localStorage.removeItem('media-finder-search-history');

            if (this.searchInput) {
                this.setupSearchListeners();
            }
        });
    }

    setupSearchListeners() {
        // Show spinner when search starts
        this.searchInput.addEventListener('htmx:beforeRequest', () => {
            if (this.spinner) {
                this.spinner.classList.remove('hidden');
            }
        });

        // Hide spinner when search completes
        this.searchInput.addEventListener('htmx:afterRequest', () => {
            if (this.spinner) {
                this.spinner.classList.add('hidden');
            }
        });

        // Add help text for advanced search syntax
        this.addSearchSuggestions();
    }

    addSearchSuggestions() {
        // Add help text for advanced search syntax
        const helpText = document.createElement('div');
        helpText.className = 'mt-2 text-xs text-gray-500';
        helpText.innerHTML = `
            <details class="cursor-pointer">
                <summary class="hover:text-gray-400">Advanced Search Tips</summary>
                <div class="mt-1 space-y-1 ml-4">
                    <p>• Use quotes for exact matches: <code class="text-blue-400">"exact phrase"</code></p>
                    <p>• Wildcard search: <code class="text-blue-400">*.mp4</code></p>
                    <p>• Multiple terms: <code class="text-blue-400">movie 2023</code></p>
                </div>
            </details>
        `;

        if (this.searchInput && this.searchInput.parentElement && this.searchInput.parentElement.parentElement) {
            this.searchInput.parentElement.parentElement.appendChild(helpText);
        }
    }
}

// Initialize search manager
const searchManager = new SearchManager();

// Expose globally for manual control
window.searchManager = searchManager;

