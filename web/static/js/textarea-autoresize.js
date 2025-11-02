// Textarea Auto-Resize System
// Automatically adjusts textarea height to fit content

class TextareaAutoResize {
    constructor() {
        this.initialized = new Set(); // Track initialized textareas
        this.resizing = new WeakSet(); // Track textareas currently being resized (prevents recursion)
        this.setupAutoResize();
    }

    setupAutoResize() {
        // Find all textareas with auto-resize attribute
        const textareas = document.querySelectorAll('textarea[data-autoresize]');

        textareas.forEach(textarea => {
            // Skip if already initialized
            if (this.initialized.has(textarea)) return;

            // Set initial height
            this.resize(textarea);

            // Add event listeners for user input
            // These are sufficient - textareas don't have child nodes to observe
            textarea.addEventListener('input', () => this.resize(textarea));
            textarea.addEventListener('change', () => this.resize(textarea));

            // Mark as initialized
            this.initialized.add(textarea);
        });
    }

    resize(textarea) {
        // Prevent recursive calls
        if (this.resizing.has(textarea)) return;

        try {
            this.resizing.add(textarea);

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
        } finally {
            this.resizing.delete(textarea);
        }
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
