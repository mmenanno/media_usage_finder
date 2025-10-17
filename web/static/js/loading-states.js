// Loading States and Indicators for HTMX Requests

class LoadingManager {
    constructor() {
        this.activeRequests = new Set();
        this.setupGlobalIndicator();
        this.setupHTMXListeners();
    }

    setupGlobalIndicator() {
        // Create global loading indicator
        const indicator = document.createElement('div');
        indicator.id = 'global-loading-indicator';
        indicator.className = 'fixed top-0 left-0 right-0 h-1 bg-blue-600 transform origin-left scale-x-0 transition-transform duration-300 z-50';
        indicator.style.transformOrigin = 'left';
        document.body.appendChild(indicator);
    }

    setupHTMXListeners() {
        // Show loading on request start
        document.body.addEventListener('htmx:beforeRequest', (event) => {
            const target = event.detail.elt;

            // Add to active requests
            this.activeRequests.add(target);

            // Show global indicator
            this.showGlobalIndicator();

            // Add loading class to the element
            target.classList.add('htmx-loading');

            // Add loading spinner to buttons
            if (target.tagName === 'BUTTON' || target.tagName === 'A') {
                this.addButtonSpinner(target);
            }

            // Add loading indicator to target container
            if (target.hasAttribute('hx-target')) {
                const targetSelector = target.getAttribute('hx-target');
                const targetEl = targetSelector === 'this' ? target : document.querySelector(targetSelector);
                if (targetEl) {
                    targetEl.classList.add('htmx-target-loading');
                }
            }
        });

        // Hide loading on request complete
        document.body.addEventListener('htmx:afterRequest', (event) => {
            const target = event.detail.elt;

            // Remove from active requests
            this.activeRequests.delete(target);

            // Hide global indicator if no active requests
            if (this.activeRequests.size === 0) {
                this.hideGlobalIndicator();
            }

            // Remove loading class
            target.classList.remove('htmx-loading');

            // Remove button spinner
            if (target.tagName === 'BUTTON' || target.tagName === 'A') {
                this.removeButtonSpinner(target);
            }

            // Remove loading indicator from target
            if (target.hasAttribute('hx-target')) {
                const targetSelector = target.getAttribute('hx-target');
                const targetEl = targetSelector === 'this' ? target : document.querySelector(targetSelector);
                if (targetEl) {
                    targetEl.classList.remove('htmx-target-loading');
                }
            }
        });
    }

    showGlobalIndicator() {
        const indicator = document.getElementById('global-loading-indicator');
        if (indicator) {
            indicator.style.transform = 'scaleX(1)';
        }
    }

    hideGlobalIndicator() {
        const indicator = document.getElementById('global-loading-indicator');
        if (indicator) {
            indicator.style.transform = 'scaleX(0)';
        }
    }

    addButtonSpinner(button) {
        // Store original content
        if (!button.dataset.originalContent) {
            button.dataset.originalContent = button.innerHTML;
        }

        // Add spinner
        const spinner = `
            <svg class="inline-block animate-spin h-4 w-4 mr-2" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
        `;

        button.innerHTML = spinner + button.textContent;
        button.disabled = true;
    }

    removeButtonSpinner(button) {
        if (button.dataset.originalContent) {
            button.innerHTML = button.dataset.originalContent;
            delete button.dataset.originalContent;
        }
        button.disabled = false;
    }
}

// Add CSS for loading states
const style = document.createElement('style');
style.textContent = `
    .htmx-loading {
        opacity: 0.6;
        pointer-events: none;
    }

    .htmx-target-loading {
        position: relative;
        min-height: 100px;
    }

    .htmx-target-loading::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0, 0, 0, 0.1);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 10;
    }

    .htmx-target-loading::after {
        content: '';
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        width: 40px;
        height: 40px;
        border: 3px solid #3b82f6;
        border-top-color: transparent;
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
        z-index: 11;
    }

    @keyframes spin {
        to { transform: translate(-50%, -50%) rotate(360deg); }
    }
`;
document.head.appendChild(style);

// Initialize loading manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.loadingManager = new LoadingManager();
});

