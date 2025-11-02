// Keyboard Shortcuts System

class KeyboardShortcuts {
    constructor() {
        this.shortcuts = {
            '/': { action: () => this.focusSearch(), description: 'Focus search' },
            's': { action: () => this.startScan(), description: 'Start scan' },
            'd': { action: () => this.goTo('/'), description: 'Go to Dashboard' },
            'f': { action: () => this.goTo('/files'), description: 'Go to Files' },
            'h': { action: () => this.goTo('/hardlinks'), description: 'Go to Hardlinks' },
            'c': { action: () => this.goTo('/config'), description: 'Go to Configuration' },
            't': { action: () => window.toggleTheme && window.toggleTheme(), description: 'Toggle theme' },
            '?': { action: () => this.showHelp(), description: 'Show keyboard shortcuts' },
            'Escape': { action: () => this.handleEscape(), description: 'Close modals/menus' }
        };

        this.helpVisible = false;
        this.setupListeners();
    }

    setupListeners() {
        document.addEventListener('keydown', (e) => {
            // Prevent Enter from submitting form when in textarea
            if (e.key === 'Enter' && e.target.tagName === 'TEXTAREA') {
                // Allow normal Enter behavior (newline) in textarea
                // Don't preventDefault - let it insert newline
                return;
            }

            // Prevent Enter from submitting forms when in input fields
            if (e.key === 'Enter' && e.target.tagName === 'INPUT') {
                e.preventDefault();
                return;
            }

            // Don't trigger ANY shortcuts when typing in input fields, textareas, or contenteditable
            // This ensures users can type all characters (including '/', 's', 'f', etc.) normally
            if (e.target.tagName === 'INPUT' ||
                e.target.tagName === 'TEXTAREA' ||
                e.target.isContentEditable) {
                return;
            }

            // Don't trigger if modifier keys are pressed (except for specific combos)
            if (e.ctrlKey || e.metaKey || e.altKey) {
                return;
            }

            const shortcut = this.shortcuts[e.key];
            if (shortcut) {
                e.preventDefault();
                shortcut.action();
            }
        });
    }

    focusSearch() {
        const searchInput = document.getElementById('search-input');
        if (searchInput) {
            searchInput.focus();
            searchInput.select();
        }
    }

    startScan() {
        // Trigger scan start with confirmation
        if (confirm('Start a new scan? This may take a while for large libraries.')) {
            fetch('/api/scan/start', { method: 'POST' })
                .then(response => response.json())
                .then(() => {
                    window.showToast && window.showToast('Scan started successfully', 'info');
                    // Reload page to show progress
                    setTimeout(() => window.location.href = '/', 500);
                })
                .catch(error => {
                    window.showToast && window.showToast('Failed to start scan', 'error');
                });
        }
    }

    goTo(path) {
        window.location.href = path;
    }

    handleEscape() {
        // Close mobile menu
        const mobileMenu = document.getElementById('mobile-menu');
        if (mobileMenu && !mobileMenu.classList.contains('hidden')) {
            window.toggleMobileMenu && window.toggleMobileMenu();
            return;
        }

        // Close file details modal
        const fileDetailsModal = document.getElementById('file-details-modal');
        if (fileDetailsModal && !fileDetailsModal.classList.contains('hidden')) {
            window.fileDetailsModal && window.fileDetailsModal.hide();
            return;
        }

        // Close shortcuts help
        if (this.helpVisible) {
            this.hideHelp();
            return;
        }
    }

    showHelp() {
        if (this.helpVisible) {
            this.hideHelp();
            return;
        }

        const helpModal = document.createElement('div');
        helpModal.id = 'shortcuts-help';
        helpModal.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 animate-fadeIn';

        const shortcutsList = Object.entries(this.shortcuts)
            .filter(([key]) => key !== 'Escape') // Don't show Escape in list
            .map(([key, config]) => `
                <div class="flex justify-between items-center py-2">
                    <span class="text-gray-300">${config.description}</span>
                    <kbd class="px-3 py-1 bg-gray-700 border border-gray-600 rounded text-sm font-mono">${key === '/' ? '/' : key.toUpperCase()}</kbd>
                </div>
            `).join('');

        helpModal.innerHTML = `
            <div class="bg-gray-800 rounded-lg shadow-xl max-w-lg w-full mx-4 transform transition-all animate-scaleIn">
                <div class="bg-gray-700 px-6 py-4 flex justify-between items-center border-b border-gray-600">
                    <h3 class="text-lg font-semibold text-white">Keyboard Shortcuts</h3>
                    <button onclick="keyboardShortcuts.hideHelp()" class="text-gray-400 hover:text-white">
                        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                        </svg>
                    </button>
                </div>
                <div class="p-6">
                    <div class="space-y-1 max-h-96 overflow-y-auto">
                        ${shortcutsList}
                    </div>
                    <div class="mt-4 pt-4 border-t border-gray-700 text-sm text-gray-400">
                        <p>Press <kbd class="px-2 py-1 bg-gray-700 border border-gray-600 rounded text-xs">ESC</kbd> to close dialogs and menus</p>
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(helpModal);
        this.helpVisible = true;

        // Click outside to close
        helpModal.addEventListener('click', (e) => {
            if (e.target === helpModal) {
                this.hideHelp();
            }
        });
    }

    hideHelp() {
        const helpModal = document.getElementById('shortcuts-help');
        if (helpModal) {
            helpModal.remove();
            this.helpVisible = false;
        }
    }
}

// Add CSS for animations
const style = document.createElement('style');
style.textContent = `
    @keyframes fadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
    }

    @keyframes scaleIn {
        from { transform: scale(0.95); opacity: 0; }
        to { transform: scale(1); opacity: 1; }
    }

    .animate-fadeIn {
        animation: fadeIn 0.2s ease-out;
    }

    .animate-scaleIn {
        animation: scaleIn 0.2s ease-out;
    }

    kbd {
        font-family: ui-monospace, monospace;
    }
`;
document.head.appendChild(style);

// Initialize keyboard shortcuts
const keyboardShortcuts = new KeyboardShortcuts();

// Expose globally
window.keyboardShortcuts = keyboardShortcuts;

// Add keyboard shortcut indicator to footer
document.addEventListener('DOMContentLoaded', () => {
    const footer = document.querySelector('footer p');
    if (footer) {
        footer.innerHTML += ' | Press <kbd class="text-xs px-2 py-1 bg-gray-800 border border-gray-700 rounded">?</kbd> for keyboard shortcuts';
    }
});

