// Toast Notification System

class ToastManager {
    constructor() {
        this.container = this.createContainer();
        this.setupHTMXListeners();
    }

    createContainer() {
        const container = document.createElement('div');
        container.id = 'toast-container';
        container.className = 'fixed top-4 right-4 z-50 space-y-2';
        container.style.maxWidth = '400px';
        document.body.appendChild(container);
        return container;
    }

    setupHTMXListeners() {
        // Listen for successful HTMX requests
        document.body.addEventListener('htmx:afterRequest', (event) => {
            const xhr = event.detail.xhr;

            // Prioritize custom toast headers - only show if present
            const toastMessage = xhr.getResponseHeader('X-Toast-Message');
            const toastType = xhr.getResponseHeader('X-Toast-Type') || 'info';

            if (toastMessage) {
                this.show(toastMessage, toastType);
                return; // Don't auto-generate if custom message exists
            }

            // Auto-generate toasts only for operations that don't send X-Toast-Message
            // This prevents duplicate notifications
            if (xhr.status >= 400) {
                // Always show error toasts if no custom message
                const errorMsg = this.parseErrorMessage(xhr);
                this.show(errorMsg, 'error');
            }
        });
    }

    parseErrorMessage(xhr) {
        try {
            const response = JSON.parse(xhr.responseText);
            const errorMsg = response.error || response.message || 'Operation failed';
            const suggestion = response.suggestion;

            // If there's a suggestion, show it in a separate toast after the error
            if (suggestion) {
                setTimeout(() => {
                    this.show(suggestion, 'info');
                }, 500);
            }

            return errorMsg;
        } catch {
            return 'Operation failed';
        }
    }

    show(message, type = 'info', options = {}) {
        const {
            duration = type === 'error' ? null : 5000, // Errors persist by default
            retryAction = null, // Function to call on retry
            dismissible = true
        } = options;

        const toast = this.createToast(message, type, retryAction, dismissible);
        this.container.appendChild(toast);

        // Animate in
        setTimeout(() => toast.classList.add('translate-x-0', 'opacity-100'), 10);

        // Auto-dismiss if duration is set
        if (duration) {
            setTimeout(() => this.dismiss(toast), duration);
        }
    }

    createToast(message, type, retryAction, dismissible) {
        const toast = document.createElement('div');
        toast.className = `transform translate-x-full opacity-0 transition-all duration-300
                          rounded-lg shadow-lg p-4 flex items-center space-x-3
                          ${this.getTypeClasses(type)}`;

        const icon = this.getIcon(type);
        const closeBtn = dismissible ? this.createCloseButton(toast) : '';
        const retryBtn = retryAction && type === 'error' ? this.createRetryButton(toast, retryAction) : '';

        toast.innerHTML = `
            ${icon}
            <span class="flex-1 text-sm font-medium">${message}</span>
            <div class="flex items-center space-x-2">
                ${retryBtn}
                ${closeBtn}
            </div>
        `;

        // Add click handler for close button
        if (dismissible) {
            const closeBtnEl = toast.querySelector('.toast-close');
            if (closeBtnEl) {
                closeBtnEl.addEventListener('click', () => this.dismiss(toast));
            }
        }

        // Add click handler for retry button
        if (retryAction) {
            const retryBtnEl = toast.querySelector('.toast-retry');
            if (retryBtnEl) {
                retryBtnEl.addEventListener('click', () => {
                    this.dismiss(toast);
                    retryAction();
                });
            }
        }

        return toast;
    }

    getTypeClasses(type) {
        const classes = {
            success: 'bg-green-600 text-white',
            error: 'bg-red-600 text-white',
            warning: 'bg-yellow-600 text-white',
            info: 'bg-blue-600 text-white'
        };
        return classes[type] || classes.info;
    }

    getIcon(type) {
        // Use shared icon constants
        return window.Icons ? window.Icons.get(type, 5) : '';
    }

    createCloseButton(toast) {
        const closeIcon = window.Icons ? window.Icons.close : '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>';
        return `<button class="toast-close text-white hover:text-gray-200 transition focus:outline-none focus:ring-2 focus:ring-white rounded" aria-label="Dismiss notification">
                    ${closeIcon}
                </button>`;
    }

    createRetryButton(toast) {
        return `<button class="toast-retry px-3 py-1 bg-white bg-opacity-20 hover:bg-opacity-30 rounded text-xs font-medium transition focus:outline-none focus:ring-2 focus:ring-white" aria-label="Retry action">
                    Retry
                </button>`;
    }

    dismiss(toast) {
        toast.classList.remove('translate-x-0', 'opacity-100');
        toast.classList.add('translate-x-full', 'opacity-0');
        setTimeout(() => toast.remove(), 300);
    }
}

// Initialize toast manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.toastManager = new ToastManager();
});

// Expose showToast globally for custom use
window.showToast = (message, type = 'info', options = {}) => {
    if (window.toastManager) {
        window.toastManager.show(message, type, options);
    }
};

