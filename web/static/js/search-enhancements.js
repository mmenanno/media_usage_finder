// Search Enhancements - Loading Indicators and History

class SearchManager {
    constructor() {
        this.searchInput = null;
        this.spinner = null;
        this.historyKey = 'media-finder-search-history';
        this.maxHistoryItems = 10;
        this.init();
    }

    init() {
        document.addEventListener('DOMContentLoaded', () => {
            this.searchInput = document.getElementById('search-input');
            this.spinner = document.getElementById('search-spinner');

            if (this.searchInput) {
                this.setupSearchListeners();
                this.setupSearchHistory();
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

            // Save to history if search was successful
            const searchValue = this.searchInput.value.trim();
            if (searchValue) {
                this.addToHistory(searchValue);
            }
        });

        // Add datalist for search suggestions
        this.addSearchSuggestions();
    }

    setupSearchHistory() {
        // Create datalist for autocomplete
        const datalist = document.createElement('datalist');
        datalist.id = 'search-history';
        document.body.appendChild(datalist);

        this.searchInput.setAttribute('list', 'search-history');
        this.updateSuggestions();
    }

    addToHistory(searchTerm) {
        let history = this.getHistory();

        // Remove if already exists
        history = history.filter(item => item !== searchTerm);

        // Add to beginning
        history.unshift(searchTerm);

        // Limit size
        if (history.length > this.maxHistoryItems) {
            history = history.slice(0, this.maxHistoryItems);
        }

        localStorage.setItem(this.historyKey, JSON.stringify(history));
        this.updateSuggestions();
    }

    getHistory() {
        try {
            const history = localStorage.getItem(this.historyKey);
            return history ? JSON.parse(history) : [];
        } catch {
            return [];
        }
    }

    updateSuggestions() {
        const datalist = document.getElementById('search-history');
        if (!datalist) return;

        const history = this.getHistory();
        datalist.innerHTML = '';

        history.forEach(term => {
            const option = document.createElement('option');
            option.value = term;
            datalist.appendChild(option);
        });
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

    clearHistory() {
        localStorage.removeItem(this.historyKey);
        this.updateSuggestions();
    }
}

// Initialize search manager
const searchManager = new SearchManager();

// Expose globally for manual control
window.searchManager = searchManager;

