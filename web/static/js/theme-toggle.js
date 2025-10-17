// Theme Toggle with localStorage Persistence
class ThemeManager {
    constructor() {
        this.currentTheme = this.getStoredTheme() || 'dark';
        this.applyTheme(this.currentTheme);
        this.createToggleButton();
    }

    getStoredTheme() {
        return localStorage.getItem('theme');
    }

    setStoredTheme(theme) {
        localStorage.setItem('theme', theme);
    }

    applyTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);

        // Update colors based on theme
        if (theme === 'light') {
            document.documentElement.classList.remove('dark');
            document.documentElement.classList.add('light');
        } else {
            document.documentElement.classList.remove('light');
            document.documentElement.classList.add('dark');
        }
    }

    toggleTheme() {
        const newTheme = this.currentTheme === 'dark' ? 'light' : 'dark';
        this.currentTheme = newTheme;
        this.setStoredTheme(newTheme);
        this.applyTheme(newTheme);
        this.updateToggleButton();
    }

    createToggleButton() {
        // Create toggle button if it doesn't exist
        if (document.getElementById('theme-toggle')) {
            this.updateToggleButton();
            return;
        }

        const button = document.createElement('button');
        button.id = 'theme-toggle';
        button.className = 'fixed top-4 right-4 z-40 p-2 rounded-lg bg-gray-800 dark:bg-gray-700 hover:bg-gray-700 dark:hover:bg-gray-600 transition-colors';
        button.setAttribute('aria-label', 'Toggle theme');
        button.setAttribute('title', 'Toggle light/dark theme');

        this.updateButtonContent(button);

        button.addEventListener('click', () => this.toggleTheme());

        document.body.appendChild(button);
    }

    updateToggleButton() {
        const button = document.getElementById('theme-toggle');
        if (button) {
            this.updateButtonContent(button);
        }
    }

    updateButtonContent(button) {
        if (this.currentTheme === 'dark') {
            button.innerHTML = `
                <svg class="w-6 h-6 text-yellow-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"></path>
                </svg>
            `;
        } else {
            button.innerHTML = `
                <svg class="w-6 h-6 text-gray-700" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"></path>
                </svg>
            `;
        }
    }
}

// Initialize theme manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.themeManager = new ThemeManager();
});

