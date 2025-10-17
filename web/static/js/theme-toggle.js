// Theme Toggle with localStorage Persistence and System Preference Detection

class ThemeManager {
    constructor() {
        this.storageKey = 'media-finder-theme';
        this.init();
    }

    init() {
        // Apply saved theme or system preference
        const savedTheme = localStorage.getItem(this.storageKey);

        if (savedTheme) {
            this.setTheme(savedTheme);
        } else {
            // Detect system preference
            const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
            this.setTheme(prefersDark ? 'dark' : 'light');
        }

        // Listen for system theme changes
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
            if (!localStorage.getItem(this.storageKey)) {
                this.setTheme(e.matches ? 'dark' : 'light');
            }
        });
    }

    setTheme(theme) {
        if (theme === 'dark') {
            document.documentElement.classList.add('dark');
        } else {
            document.documentElement.classList.remove('dark');
        }
        localStorage.setItem(this.storageKey, theme);
    }

    toggle() {
        const currentTheme = document.documentElement.classList.contains('dark') ? 'dark' : 'light';
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
        this.setTheme(newTheme);
        return newTheme;
    }

    getCurrentTheme() {
        return document.documentElement.classList.contains('dark') ? 'dark' : 'light';
    }
}

// Initialize theme manager
const themeManager = new ThemeManager();

// Expose toggle function globally
window.toggleTheme = () => themeManager.toggle();
window.getCurrentTheme = () => themeManager.getCurrentTheme();
