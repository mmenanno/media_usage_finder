// Modal System for Confirmations and Dialogs
class ModalManager {
    constructor() {
        this.createModalContainer();
        this.replaceNativeConfirm();
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
        modal.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center animate-fadeIn';

        modal.innerHTML = `
            <div class="bg-gray-800 rounded-lg shadow-xl max-w-md w-full mx-4 transform transition-all animate-scaleIn">
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
                            class="px-4 py-2 rounded-lg font-medium transition-colors ${this.getButtonClass(btn.class)}"
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

        // Close on backdrop click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                this.hide();
                if (buttons[0]) buttons[0].action();
            }
        });

        // Close on Escape key
        const escapeHandler = (e) => {
            if (e.key === 'Escape') {
                this.hide();
                if (buttons[0]) buttons[0].action();
                document.removeEventListener('keydown', escapeHandler);
            }
        };
        document.addEventListener('keydown', escapeHandler);

        return modal;
    }

    getIcon(type, color) {
        const icons = {
            info: `<svg class="w-6 h-6 text-${color}-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>`,
            success: `<svg class="w-6 h-6 text-${color}-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>`,
            warning: `<svg class="w-6 h-6 text-${color}-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>`,
            error: `<svg class="w-6 h-6 text-${color}-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>`,
            confirm: `<svg class="w-6 h-6 text-${color}-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>`
        };
        return icons[type] || icons.info;
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
        const container = document.getElementById('modal-container');
        container.classList.add('hidden');
        container.innerHTML = '';
    }
}

// Initialize modal manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.modalManager = new ModalManager();
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

