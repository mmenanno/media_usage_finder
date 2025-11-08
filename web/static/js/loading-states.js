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
        const sizeMap = {
            small: 4,
            medium: 5,
            large: 8
        };

        const iconSize = sizeMap[size] || sizeMap.medium;

        return `
            <div class="flex items-center space-x-2">
                ${Icons.get('spinner', iconSize)}
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
