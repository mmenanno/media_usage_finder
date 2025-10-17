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
            return response.error || response.message || 'Operation failed';
        } catch {
            return 'Operation failed';
        }
    }

    show(message, type = 'info') {
        const toast = this.createToast(message, type);
        this.container.appendChild(toast);

        // Animate in
        setTimeout(() => toast.classList.add('translate-x-0', 'opacity-100'), 10);

        // Auto-dismiss after 5 seconds
        setTimeout(() => this.dismiss(toast), 5000);
    }

    createToast(message, type) {
        const toast = document.createElement('div');
        toast.className = `transform translate-x-full opacity-0 transition-all duration-300
                          rounded-lg shadow-lg p-4 flex items-center space-x-3
                          ${this.getTypeClasses(type)}`;

        const icon = this.getIcon(type);
        const closeBtn = this.createCloseButton(toast);

        toast.innerHTML = `
            ${icon}
            <span class="flex-1 text-sm font-medium">${message}</span>
            ${closeBtn}
        `;

        // Add click handler for close button
        toast.querySelector('.toast-close').addEventListener('click', () => this.dismiss(toast));

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
        const icons = {
            success: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>',
            error: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>',
            warning: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>',
            info: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>'
        };
        return icons[type] || icons.info;
    }

    createCloseButton(toast) {
        return `<button class="toast-close text-white hover:text-gray-200 transition">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                    </svg>
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
window.showToast = (message, type = 'info') => {
    if (window.toastManager) {
        window.toastManager.show(message, type);
    }
};

