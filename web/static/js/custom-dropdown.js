// Custom Dropdown Component
class CustomDropdown {
    constructor(element) {
        this.dropdown = element;
        this.button = element.querySelector('[data-dropdown-button]');
        this.menu = element.querySelector('[data-dropdown-menu]');
        this.input = element.querySelector('[data-dropdown-input]');
        this.options = element.querySelectorAll('[data-dropdown-option]');
        this.isOpen = false;

        this.init();
    }

    init() {
        // Toggle dropdown on button click
        this.button.addEventListener('click', (e) => {
            e.stopPropagation();
            this.toggle();
        });

        // Handle option selection
        this.options.forEach(option => {
            option.addEventListener('click', (e) => {
                e.stopPropagation();
                this.selectOption(option);
            });
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!this.dropdown.contains(e.target)) {
                this.close();
            }
        });

        // Close on escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });
    }

    toggle() {
        this.isOpen ? this.close() : this.open();
    }

    open() {
        this.isOpen = true;
        this.menu.classList.remove('hidden');
        this.button.setAttribute('aria-expanded', 'true');
    }

    close() {
        this.isOpen = false;
        this.menu.classList.add('hidden');
        this.button.setAttribute('aria-expanded', 'false');
    }

    selectOption(option) {
        const value = option.dataset.value;
        const text = option.textContent.trim();

        // Update hidden input value
        this.input.value = value;

        // Update button text
        const buttonText = this.button.querySelector('[data-dropdown-text]');
        buttonText.textContent = text;

        // Update selected state
        this.options.forEach(opt => {
            opt.classList.remove('bg-blue-600');
            opt.setAttribute('aria-selected', 'false');
        });
        option.classList.add('bg-blue-600');
        option.setAttribute('aria-selected', 'true');

        this.close();

        // Trigger change event
        this.input.dispatchEvent(new Event('change', { bubbles: true }));

        // Auto-submit form for limit dropdown
        if (this.input.name === 'limit') {
            const form = this.input.closest('form');
            if (form) {
                form.submit();
            }
        }
    }
}

// Initialize all custom dropdowns on page load
document.addEventListener('DOMContentLoaded', () => {
    const dropdowns = document.querySelectorAll('[data-custom-dropdown]');
    dropdowns.forEach(dropdown => new CustomDropdown(dropdown));
});
