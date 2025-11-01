// Textarea Auto-Resize System
// Automatically adjusts textarea height to fit content

class TextareaAutoResize {
    constructor() {
        this.observers = new Map(); // Track observers to prevent duplicates
        this.setupAutoResize();
    }

    setupAutoResize() {
        // Find all textareas with auto-resize attribute
        const textareas = document.querySelectorAll('textarea[data-autoresize]');

        textareas.forEach(textarea => {
            // Skip if already initialized
            if (this.observers.has(textarea)) return;

            // Set initial height
            this.resize(textarea);

            // Add event listeners
            const inputHandler = () => this.resize(textarea);
            const changeHandler = () => this.resize(textarea);

            textarea.addEventListener('input', inputHandler);
            textarea.addEventListener('change', changeHandler);

            // Handle dynamic content loading (for HTMX updates)
            // Don't watch attributes to avoid infinite loop when we modify style.height
            const observer = new MutationObserver(() => this.resize(textarea));
            observer.observe(textarea, {
                childList: true,
                subtree: true
            });

            // Store observer for cleanup
            this.observers.set(textarea, { observer, inputHandler, changeHandler });
        });
    }

    resize(textarea) {
        // Reset height to auto to get the correct scrollHeight
        textarea.style.height = 'auto';

        // Set min-height to prevent shrinking below a reasonable size
        const minHeight = 80; // ~3 rows
        const maxHeight = 600; // Maximum height before scrolling

        // Calculate new height based on scrollHeight
        let newHeight = textarea.scrollHeight;

        // Apply min/max constraints
        newHeight = Math.max(minHeight, Math.min(maxHeight, newHeight));

        // Set the new height
        textarea.style.height = newHeight + 'px';
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.textareaAutoResize = new TextareaAutoResize();
});

// Setup new textareas after HTMX swaps (reuse existing instance)
document.body.addEventListener('htmx:afterSwap', () => {
    if (window.textareaAutoResize) {
        window.textareaAutoResize.setupAutoResize();
    }
});
