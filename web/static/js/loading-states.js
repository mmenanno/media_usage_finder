// Loading State Manager for Consistent UI
class LoadingStateManager {
    constructor() {
        this.activeOperations = new Map(); // Track active operations by ID
    }

    /**
     * Show loading state for a specific element or operation
     * @param {string} operationId - Unique identifier for the operation
     * @param {HTMLElement|string} target - Target element or selector
     * @param {Object} options - Configuration options
     */
    show(operationId, target, options = {}) {
        const {
            message = 'Loading...',
            disableElement = true,
            showSpinner = true,
            size = 'medium' // small, medium, large
        } = options;

        const element = typeof target === 'string' ? document.querySelector(target) : target;
        if (!element) return;

        // Store original state for restoration
        this.activeOperations.set(operationId, {
            element,
            originalContent: element.innerHTML,
            originalDisabled: element.disabled,
            originalClasses: element.className,
        });

        // Disable element if requested
        if (disableElement && (element.tagName === 'BUTTON' || element.tagName === 'INPUT')) {
            element.disabled = true;
            element.classList.add('opacity-50', 'cursor-not-allowed');
        }

        // Show spinner if requested
        if (showSpinner) {
            const spinner = this.createSpinner(size, message);

            if (element.tagName === 'BUTTON') {
                // For buttons, replace content with spinner
                element.innerHTML = spinner;
            } else {
                // For containers, add spinner
                const loadingDiv = document.createElement('div');
                loadingDiv.className = 'loading-overlay';
                loadingDiv.setAttribute('data-operation-id', operationId);
                loadingDiv.innerHTML = spinner;
                element.appendChild(loadingDiv);
            }
        }
    }

    /**
     * Hide loading state and restore original element state
     * @param {string} operationId - Unique identifier for the operation
     */
    hide(operationId) {
        const state = this.activeOperations.get(operationId);
        if (!state) return;

        const { element, originalContent, originalDisabled, originalClasses } = state;

        // Restore element state
        if (element.tagName === 'BUTTON' || element.tagName === 'INPUT') {
            element.disabled = originalDisabled;
            element.className = originalClasses;
            if (element.tagName === 'BUTTON') {
                element.innerHTML = originalContent;
            }
        }

        // Remove overlay if exists
        const overlay = element.querySelector(`[data-operation-id="${operationId}"]`);
        if (overlay) {
            overlay.remove();
        }

        this.activeOperations.delete(operationId);
    }

    /**
     * Create a spinner element
     * @param {string} size - Spinner size (small, medium, large)
     * @param {string} message - Loading message
     * @returns {string} HTML string for spinner
     */
    createSpinner(size, message) {
        const sizes = {
            small: 'h-4 w-4',
            medium: 'h-5 w-5',
            large: 'h-8 w-8'
        };

        const spinnerSize = sizes[size] || sizes.medium;

        return `
            <div class="flex items-center space-x-2">
                <svg class="animate-spin ${spinnerSize}" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                ${message ? `<span class="text-sm">${message}</span>` : ''}
            </div>
        `;
    }

    /**
     * Check if an operation is currently loading
     * @param {string} operationId - Unique identifier for the operation
     * @returns {boolean}
     */
    isLoading(operationId) {
        return this.activeOperations.has(operationId);
    }

    /**
     * Clear all active loading states (useful for cleanup)
     */
    clearAll() {
        for (const [operationId] of this.activeOperations) {
            this.hide(operationId);
        }
    }

    /**
     * Create a skeleton loader for content placeholders
     * @param {string} type - Type of skeleton (text, card, table)
     * @returns {string} HTML string for skeleton
     */
    createSkeleton(type = 'text') {
        const skeletons = {
            text: `
                <div class="animate-pulse space-y-2">
                    <div class="h-4 bg-gray-700 rounded w-3/4"></div>
                    <div class="h-4 bg-gray-700 rounded w-1/2"></div>
                </div>
            `,
            card: `
                <div class="animate-pulse bg-gray-800 rounded-lg p-6">
                    <div class="h-6 bg-gray-700 rounded w-1/4 mb-4"></div>
                    <div class="space-y-3">
                        <div class="h-4 bg-gray-700 rounded"></div>
                        <div class="h-4 bg-gray-700 rounded w-5/6"></div>
                    </div>
                </div>
            `,
            table: `
                <div class="animate-pulse space-y-2">
                    <div class="h-10 bg-gray-700 rounded"></div>
                    <div class="h-10 bg-gray-700 rounded"></div>
                    <div class="h-10 bg-gray-700 rounded"></div>
                </div>
            `
        };

        return skeletons[type] || skeletons.text;
    }
}

// Initialize loading state manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.loadingStateManager = new LoadingStateManager();
});

// Expose globally
window.LoadingStateManager = LoadingStateManager;
