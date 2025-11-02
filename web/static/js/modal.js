// Modal System for Confirmations and Dialogs
class ModalManager {
    constructor() {
        this.createModalContainer();
        this.replaceNativeConfirm();
        this.currentEscapeHandler = null;
        this.currentBackdropHandler = null;
    }

    createModalContainer() {
        const container = document.createElement('div');
        container.id = 'modal-container';
        container.className = 'fixed inset-0 z-50 hidden';
        document.body.appendChild(container);
    }

    replaceNativeConfirm() {
        // Intercept HTMX confirm events
        document.body.addEventListener('htmx:confirm', (event) => {
            // Debug logging
            console.log('htmx:confirm triggered', {
                question: event.detail.question,
                target: event.target,
                elt: event.detail.elt,
                issuer: event.detail.issuer
            });

            event.preventDefault();

            const question = event.detail.question || 'Are you sure?';

            this.confirm(question).then((result) => {
                if (result) {
                    event.detail.issueRequest();
                }
            });
        });
    }

    confirm(message, title = 'Confirm Action') {
        return new Promise((resolve) => {
            const modal = this.createModal({
                title,
                message,
                type: 'confirm',
                buttons: [
                    { text: 'Cancel', class: 'secondary', action: () => resolve(false) },
                    { text: 'Confirm', class: 'primary', action: () => resolve(true) }
                ]
            });

            this.show(modal);
        });
    }

    alert(message, title = 'Notice', type = 'info') {
        return new Promise((resolve) => {
            const modal = this.createModal({
                title,
                message,
                type,
                buttons: [
                    { text: 'OK', class: 'primary', action: () => resolve(true) }
                ]
            });

            this.show(modal);
        });
    }

    createModal({ title, message, type, buttons }) {
        const typeColors = {
            info: 'blue',
            success: 'green',
            warning: 'yellow',
            error: 'red',
            confirm: 'blue'
        };

        const color = typeColors[type] || 'blue';

        const modal = document.createElement('div');
        modal.className = 'fixed inset-0 flex items-center justify-center animate-fadeIn';

        modal.innerHTML = `
            <div class="bg-gray-800 border border-gray-600 rounded-lg shadow-xl max-w-md w-full mx-4 transform transition-all animate-scaleIn">
                <div class="p-6">
                    <div class="flex items-start space-x-4">
                        ${this.getIcon(type, color)}
                        <div class="flex-1">
                            <h3 class="text-lg font-semibold text-white mb-2">${title}</h3>
                            <p class="text-gray-300">${message}</p>
                        </div>
                    </div>
                </div>
                <div class="bg-gray-700 px-6 py-4 rounded-b-lg flex justify-end space-x-3">
                    ${buttons.map((btn, i) => `
                        <button
                            data-action="${i}"
                            class="px-4 py-2 rounded-lg font-medium transition-colors cursor-pointer ${this.getButtonClass(btn.class)}"
                        >
                            ${btn.text}
                        </button>
                    `).join('')}
                </div>
            </div>
        `;

        // Add click handlers
        buttons.forEach((btn, i) => {
            const buttonEl = modal.querySelector(`[data-action="${i}"]`);
            buttonEl.addEventListener('click', () => {
                this.hide();
                btn.action();
            });
        });

        // Store escape handler reference for cleanup
        const escapeHandler = (e) => {
            if (e.key === 'Escape') {
                this.hide();
            }
        };

        // Store backdrop click handler for cleanup
        const backdropHandler = (e) => {
            if (e.target === modal) {
                this.hide();
            }
        };

        // Store handlers for cleanup
        this.currentEscapeHandler = escapeHandler;
        this.currentBackdropHandler = backdropHandler;

        // Close on backdrop click (without triggering any action)
        modal.addEventListener('click', backdropHandler);

        // Close on Escape key (without triggering any action)
        document.addEventListener('keydown', escapeHandler);

        return modal;
    }

    getIcon(type, color) {
        // Use shared icon constants with color
        return window.Icons.getWithColor(type, `text-${color}-500`, 6);
    }

    getButtonClass(type) {
        if (type === 'primary') {
            return 'bg-blue-600 hover:bg-blue-700 text-white';
        }
        return 'bg-gray-600 hover:bg-gray-500 text-white';
    }

    show(modal) {
        const container = document.getElementById('modal-container');
        container.innerHTML = '';
        container.appendChild(modal);
        container.classList.remove('hidden');
    }

    hide() {
        // Clean up event handlers to prevent memory leaks
        if (this.currentEscapeHandler) {
            document.removeEventListener('keydown', this.currentEscapeHandler);
            this.currentEscapeHandler = null;
        }

        const container = document.getElementById('modal-container');
        container.classList.add('hidden');
        container.innerHTML = '';
    }
}

// Initialize modal manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    if (!window.modalManager) {
        console.log('Initializing ModalManager');
        window.modalManager = new ModalManager();
    } else {
        console.warn('ModalManager already initialized - skipping');
    }
});

// Expose for manual use
window.confirmDialog = (message, title) => {
    if (window.modalManager) {
        return window.modalManager.confirm(message, title);
    }
    return Promise.resolve(confirm(message));
};

window.alertDialog = (message, title, type) => {
    if (window.modalManager) {
        return window.modalManager.alert(message, title, type);
    }
    return Promise.resolve(alert(message));
};

