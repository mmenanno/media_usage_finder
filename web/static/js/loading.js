// Loading Indicators for Long Operations
class LoadingManager {
    constructor() {
        this.activeOperations = new Set();
        this.setupHTMXIndicators();
        this.createGlobalSpinner();
    }

    setupHTMXIndicators() {
        // Show loading on HTMX request start
        document.body.addEventListener('htmx:beforeRequest', (event) => {
            const target = event.detail.elt;
            this.showLoading(target);
        });

        // Hide loading on HTMX request complete
        document.body.addEventListener('htmx:afterRequest', (event) => {
            const target = event.detail.elt;
            this.hideLoading(target);
        });

        // Handle errors
        document.body.addEventListener('htmx:responseError', (event) => {
            const target = event.detail.elt;
            this.hideLoading(target);
        });
    }

    createGlobalSpinner() {
        const spinner = document.createElement('div');
        spinner.id = 'global-loading';
        spinner.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 hidden';
        spinner.innerHTML = `
            <div class="bg-gray-800 rounded-lg p-6 flex flex-col items-center space-y-4">
                <svg class="animate-spin h-12 w-12 text-blue-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                <p class="text-white text-lg font-medium" id="loading-message">Loading...</p>
            </div>
        `;
        document.body.appendChild(spinner);
    }

    showLoading(element, message = 'Loading...') {
        // Add loading class to element
        if (element) {
            element.classList.add('loading');
            element.style.opacity = '0.6';
            element.style.pointerEvents = 'none';
        }

        // Track operation
        const opId = element?.id || 'global';
        this.activeOperations.add(opId);

        // Show global spinner for certain operations
        if (this.shouldShowGlobalSpinner(element)) {
            this.showGlobalSpinner(message);
        }
    }

    hideLoading(element) {
        // Remove loading class from element
        if (element) {
            element.classList.remove('loading');
            element.style.opacity = '';
            element.style.pointerEvents = '';
        }

        // Remove from active operations
        const opId = element?.id || 'global';
        this.activeOperations.delete(opId);

        // Hide global spinner if no operations
        if (this.activeOperations.size === 0) {
            this.hideGlobalSpinner();
        }
    }

    shouldShowGlobalSpinner(element) {
        // Show global spinner for scan operations, bulk deletes, etc.
        const path = element?.getAttribute('hx-post') || element?.getAttribute('hx-get') || '';
        return path.includes('/scan/start') ||
               path.includes('/delete') ||
               path.includes('/mark-rescan');
    }

    showGlobalSpinner(message = 'Loading...') {
        const spinner = document.getElementById('global-loading');
        const messageEl = document.getElementById('loading-message');
        if (spinner) {
            messageEl.textContent = message;
            spinner.classList.remove('hidden');
        }
    }

    hideGlobalSpinner() {
        const spinner = document.getElementById('global-loading');
        if (spinner) {
            spinner.classList.add('hidden');
        }
    }

    // Public method for manual control
    show(message) {
        this.showGlobalSpinner(message);
    }

    hide() {
        this.hideGlobalSpinner();
        this.activeOperations.clear();
    }
}

// Initialize loading manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.loadingManager = new LoadingManager();
});

// Expose for manual use
window.showLoading = (message) => {
    if (window.loadingManager) {
        window.loadingManager.show(message);
    }
};

window.hideLoading = () => {
    if (window.loadingManager) {
        window.loadingManager.hide();
    }
};

