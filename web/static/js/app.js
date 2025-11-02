// Media Usage Finder - Client-side JavaScript

// Handle HTMX events
document.body.addEventListener('htmx:afterSwap', function(evt) {
    // Custom animations or processing after HTMX swaps
});

// Note: Confirmation dialogs are handled by modal.js

// Format file sizes
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Auto-refresh progress when scan is running
let progressInterval = null;

function startProgressMonitoring() {
    if (progressInterval) return;

    progressInterval = setInterval(() => {
        const progressContainer = document.getElementById('progress-container');
        if (progressContainer) {
            htmx.trigger(progressContainer, 'htmx:trigger');
        }
    }, 2000);
}

function stopProgressMonitoring() {
    if (progressInterval) {
        clearInterval(progressInterval);
        progressInterval = null;
    }
}

// Global network error handler
window.addEventListener('unhandledrejection', (event) => {
    // Handle unhandled promise rejections (often network errors)
    const error = event.reason;

    if (error && error.name === 'TypeError' && error.message.includes('fetch')) {
        event.preventDefault(); // Prevent default console error

        // Check if online
        if (!navigator.onLine) {
            window.showToast && window.showToast('No internet connection. Please check your network.', 'error');
        } else {
            window.showToast && window.showToast('Network error occurred. Please try again.', 'error');
        }
    }
});

// Monitor online/offline status
window.addEventListener('offline', () => {
    window.showToast && window.showToast('You are now offline. Some features may not work.', 'warning');
});

window.addEventListener('online', () => {
    window.showToast && window.showToast('You are back online.', 'success');
});

// Form dirty state tracking for config page
let formIsDirty = false;
let originalFormData = null;

function initFormDirtyTracking() {
    const configForm = document.querySelector('form[hx-post="/api/config/save"]');
    if (!configForm) return;

    // Store original form data
    originalFormData = new FormData(configForm);

    // Track changes to any form input
    configForm.addEventListener('input', () => {
        formIsDirty = true;
    });

    configForm.addEventListener('change', () => {
        formIsDirty = true;
    });

    // Warn before leaving page with unsaved changes
    window.addEventListener('beforeunload', (e) => {
        if (formIsDirty) {
            e.preventDefault();
            e.returnValue = ''; // Required for Chrome
            return ''; // Required for some browsers
        }
    });

    // Reset dirty state on successful save
    document.body.addEventListener('htmx:afterRequest', (event) => {
        // Check if this is the config save endpoint
        if (event.detail.pathInfo.requestPath === '/api/config/save') {
            // Check if request was successful
            if (event.detail.successful) {
                formIsDirty = false;
            }
        }
    });
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', initFormDirtyTracking);

// Application initialization is handled by individual modules
// (modal.js, notifications.js, batch-selection.js, etc.)

