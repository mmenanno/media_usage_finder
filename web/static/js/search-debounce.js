// Search Debouncing for Performance
class SearchDebouncer {
    constructor(delay = 300) {
        this.delay = delay;
        this.timeoutId = null;
        this.setupSearchInputs();
    }

    setupSearchInputs() {
        const searchInputs = document.querySelectorAll('input[type="search"], input[name="search"]');

        searchInputs.forEach(input => {
            // Store original form submit if exists
            const form = input.closest('form');

            if (form) {
                // Prevent immediate form submission on input
                input.addEventListener('input', (e) => {
                    clearTimeout(this.timeoutId);

                    // Show loading indicator
                    this.showLoadingIndicator(input);

                    this.timeoutId = setTimeout(() => {
                        this.hideLoadingIndicator(input);
                        // Trigger form submission or HTMX request
                        if (form.hasAttribute('hx-get')) {
                            htmx.trigger(form, 'submit');
                        } else {
                            form.submit();
                        }
                    }, this.delay);
                });
            }
        });
    }

    showLoadingIndicator(input) {
        // Reuse existing indicator if present
        let indicator = input.nextElementSibling;
        if (indicator && indicator.classList.contains('search-loading')) {
            indicator.classList.remove('hidden');
            return;
        }

        // Create new indicator only if needed
        indicator = document.createElement('div');
        indicator.className = 'search-loading absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-400';
        indicator.innerHTML = Icons.get('spinner', 4);

        // Make input parent relative
        const parent = input.parentElement;
        if (parent && !parent.classList.contains('relative')) {
            parent.classList.add('relative');
        }

        input.after(indicator);
    }

    hideLoadingIndicator(input) {
        const indicator = input.nextElementSibling;
        if (indicator && indicator.classList.contains('search-loading')) {
            indicator.classList.add('hidden');
        }
    }
}

// Initialize search debouncer when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.searchDebouncer = new SearchDebouncer(300);
});

