// Keyboard Shortcuts and Navigation
class KeyboardManager {
    constructor() {
        this.shortcuts = {
            '/': () => this.focusSearch(),
            's': () => this.startScan(),
            'Escape': () => this.closeModals(),
            'ArrowLeft': () => this.previousPage(),
            'ArrowRight': () => this.nextPage(),
        };

        this.setupListeners();
        this.showHelpOnStartup();
    }

    setupListeners() {
        document.addEventListener('keydown', (e) => {
            // Don't trigger shortcuts when typing in inputs
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') {
                // Allow Escape to unfocus input
                if (e.key === 'Escape') {
                    e.target.blur();
                }
                return;
            }

            // Check if shortcut exists
            const handler = this.shortcuts[e.key];
            if (handler) {
                e.preventDefault();
                handler();
            }

            // Show help with '?'
            if (e.key === '?') {
                e.preventDefault();
                this.showHelp();
            }
        });
    }

    focusSearch() {
        const searchInput = document.querySelector('input[type="search"], input[name="search"]');
        if (searchInput) {
            searchInput.focus();
            searchInput.select();
        }
    }

    startScan() {
        const scanBtn = document.querySelector('[data-action="start-scan"]');
        if (scanBtn) {
            scanBtn.click();
        }
    }

    closeModals() {
        // Close any open modals
        const modals = document.querySelectorAll('.modal, [role="dialog"]');
        modals.forEach(modal => {
            modal.classList.add('hidden');
        });
    }

    previousPage() {
        const prevLink = document.querySelector('a[rel="prev"], .pagination-prev');
        if (prevLink && !prevLink.classList.contains('disabled')) {
            prevLink.click();
        }
    }

    nextPage() {
        const nextLink = document.querySelector('a[rel="next"], .pagination-next');
        if (nextLink && !nextLink.classList.contains('disabled')) {
            nextLink.click();
        }
    }

    showHelp() {
        const helpText = `
Keyboard Shortcuts:
─────────────────────
/       Focus search
s       Start scan
←  →    Previous/Next page
Esc     Close modals / Unfocus
?       Show this help

Press any key to close
        `;

        if (window.toastManager) {
            // Create a custom help modal instead of toast
            const helpModal = document.createElement('div');
            helpModal.className = 'fixed inset-0 bg-black/75 bg-opacity-50 flex items-center justify-center z-50';
            helpModal.innerHTML = `
                <div class="bg-gray-800 text-white rounded-lg p-6 max-w-md">
                    <h3 class="text-xl font-bold mb-4">Keyboard Shortcuts</h3>
                    <pre class="text-sm text-gray-300 whitespace-pre">${helpText}</pre>
                </div>
            `;

            document.body.appendChild(helpModal);

            const closeHelp = () => {
                helpModal.remove();
                document.removeEventListener('keydown', closeHelp);
            };

            setTimeout(() => {
                document.addEventListener('keydown', closeHelp);
            }, 100);

            helpModal.addEventListener('click', closeHelp);
        }
    }

    showHelpOnStartup() {
        // Show hint about keyboard shortcuts on first visit
        const hasSeenHint = localStorage.getItem('keyboard-hint-seen');
        if (!hasSeenHint) {
            setTimeout(() => {
                if (window.toastManager) {
                    window.toastManager.show('Press ? for keyboard shortcuts', 'info');
                    localStorage.setItem('keyboard-hint-seen', 'true');
                }
            }, 2000);
        }
    }
}

// Initialize keyboard manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.keyboardManager = new KeyboardManager();
});

