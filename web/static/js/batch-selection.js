// Batch Selection System for Files

class BatchSelection {
    constructor() {
        this.selectedFiles = new Set();
        this.lastSelectedIndex = null;
        this.tableChangeHandler = null; // Store handler for cleanup
        this.setupEventListeners();
    }

    setupEventListeners() {
        // Initialize immediately if DOM is already loaded, otherwise wait for event
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => {
                this.init();
            });
        } else {
            // DOM is already loaded (since this script loads at end of body)
            this.init();
        }

        // Listen for table updates (HTMX swaps)
        document.body.addEventListener('htmx:afterSwap', (event) => {
            // Handle full table swaps or tbody swaps (e.g., from infinite scroll)
            if (event.detail.target.id === 'files-table' ||
                (event.target && event.target.matches && event.target.matches('#files-table tbody'))) {
                this.init();
            }
        });

        // Clear selection on navigation (prevent memory leak)
        document.body.addEventListener('htmx:beforeRequest', (event) => {
            // Clear selection when navigating away from files page
            const url = event.detail.path;
            if (url && !url.includes('/files') && this.selectedFiles.size > 0) {
                this.clearSelection();
            }
        });

        // Clear selection on page swap to prevent stale state
        document.body.addEventListener('htmx:beforeSwap', (event) => {
            // Clear selection when main content is being replaced
            if (event.detail.target && (
                event.detail.target.id === 'main-content' ||
                event.detail.target.id === 'files-table'
            ) && !event.detail.requestConfig?.path?.includes('/files')) {
                this.selectedFiles.clear();
                this.lastSelectedIndex = null;
            }
        });

        // Add keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            // Only handle shortcuts on files page
            if (!window.location.pathname.includes('/files')) return;

            // Don't intercept shortcuts when typing in input fields, textareas, or contenteditable
            if (e.target.tagName === 'INPUT' ||
                e.target.tagName === 'TEXTAREA' ||
                e.target.isContentEditable) {
                return;
            }

            // Ctrl+A or Cmd+A: Select all
            if ((e.ctrlKey || e.metaKey) && e.key === 'a') {
                const table = document.getElementById('files-table');
                if (table) {
                    e.preventDefault();
                    this.handleSelectAll(true);
                }
            }

            // Escape: Clear selection
            if (e.key === 'Escape' && this.selectedFiles.size > 0) {
                e.preventDefault();
                this.clearSelection();
            }

            // ? key: Show keyboard shortcuts help
            if (e.key === '?' && !e.ctrlKey && !e.metaKey && !e.altKey) {
                e.preventDefault();
                this.showKeyboardShortcutsHelp();
            }
        });
    }

    init() {
        // Add select all checkbox to header
        this.addSelectAllCheckbox();

        // Add individual checkboxes to each row
        this.addRowCheckboxes();

        // Create bulk actions toolbar
        this.createBulkActionsToolbar();

        this.updateUI();
    }

    addSelectAllCheckbox() {
        const thead = document.querySelector('#files-table thead tr');
        if (!thead || thead.querySelector('.select-all-cell')) return;

        const th = document.createElement('th');
        th.className = 'select-all-cell px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider';
        th.innerHTML = `
            <input
                type="checkbox"
                id="select-all"
                aria-label="Select all files"
                class="w-4 h-4 bg-gray-700 border-gray-600 rounded">
        `;

        thead.insertBefore(th, thead.firstChild);

        // Add event listener
        const selectAll = th.querySelector('#select-all');
        selectAll.addEventListener('change', (e) => {
            this.handleSelectAll(e.target.checked);
        });
    }

    addRowCheckboxes() {
        const tbody = document.querySelector('#files-table tbody');
        if (!tbody) return;

        const rows = tbody.querySelectorAll('tr');
        rows.forEach((row, index) => {
            // Skip empty state row
            if (row.querySelector('[colspan]')) return;

            // Skip if checkbox already exists
            if (row.querySelector('.file-checkbox-cell')) return;

            const fileId = this.extractFileId(row);
            if (!fileId) return;

            const td = document.createElement('td');
            td.className = 'file-checkbox-cell px-4 py-4';
            td.innerHTML = `
                <input
                    type="checkbox"
                    class="file-checkbox w-4 h-4 bg-gray-700 border-gray-600 rounded"
                    data-file-id="${fileId}"
                    data-row-index="${index}"
                    aria-label="Select file">
            `;

            row.insertBefore(td, row.firstChild);
        });

        // Use event delegation - single listener for all checkboxes
        // This is more efficient than individual listeners per checkbox
        tbody.removeEventListener('change', this.tableChangeHandler);
        this.tableChangeHandler = (e) => {
            if (e.target.classList.contains('file-checkbox')) {
                const index = parseInt(e.target.dataset.rowIndex, 10);
                this.handleCheckboxChange(e, index);
            }
        };
        tbody.addEventListener('change', this.tableChangeHandler);
    }

    extractFileId(row) {
        // Extract file ID from the delete button
        const deleteBtn = row.querySelector('[hx-delete]');
        if (!deleteBtn) return null;

        const url = deleteBtn.getAttribute('hx-delete');
        const match = url.match(/id=(\d+)/);
        return match ? match[1] : null;
    }

    handleSelectAll(checked) {
        const checkboxes = document.querySelectorAll('.file-checkbox');
        checkboxes.forEach(cb => {
            cb.checked = checked;
            const fileId = cb.dataset.fileId;
            if (checked) {
                this.selectedFiles.add(fileId);
            } else {
                this.selectedFiles.delete(fileId);
            }
        });
        this.updateUI();
    }

    handleCheckboxChange(event, index) {
        const checkbox = event.target;
        const fileId = checkbox.dataset.fileId;

        // Handle shift-click for range selection
        if (event.shiftKey && this.lastSelectedIndex !== null) {
            const start = Math.min(this.lastSelectedIndex, index);
            const end = Math.max(this.lastSelectedIndex, index);

            const checkboxes = document.querySelectorAll('.file-checkbox');
            for (let i = start; i <= end && i < checkboxes.length; i++) {
                const cb = checkboxes[i];
                cb.checked = checkbox.checked;
                const id = cb.dataset.fileId;
                if (checkbox.checked) {
                    this.selectedFiles.add(id);
                } else {
                    this.selectedFiles.delete(id);
                }
            }
        } else {
            // Normal single selection
            if (checkbox.checked) {
                this.selectedFiles.add(fileId);
            } else {
                this.selectedFiles.delete(fileId);
            }
        }

        this.lastSelectedIndex = index;
        this.updateUI();
    }

    createBulkActionsToolbar() {
        // Remove existing toolbar if any
        const existing = document.getElementById('bulk-actions-toolbar');
        if (existing) existing.remove();

        const toolbar = document.createElement('div');
        toolbar.id = 'bulk-actions-toolbar';
        toolbar.className = 'fixed bottom-0 left-0 right-0 bg-gray-800 border-t-2 border-blue-500 shadow-2xl transform translate-y-full transition-transform duration-300 z-40';
        toolbar.setAttribute('role', 'region');
        toolbar.setAttribute('aria-label', 'Bulk actions toolbar');
        toolbar.innerHTML = `
            <div class="container mx-auto px-4 py-4">
                <div class="flex items-center justify-between">
                    <div class="flex items-center space-x-4">
                        <span class="text-sm text-gray-300" role="status" aria-live="polite">
                            <span id="selected-count" class="font-bold text-blue-400">0</span> files selected
                        </span>
                        <button
                            onclick="batchSelection.clearSelection()"
                            aria-label="Clear selection"
                            class="text-sm text-gray-400 hover:text-white transition">
                            Clear selection
                        </button>
                        <button
                            onclick="batchSelection.showKeyboardShortcutsHelp()"
                            aria-label="Show keyboard shortcuts"
                            class="text-xs text-gray-500 hover:text-gray-300 transition flex items-center space-x-1"
                            title="Keyboard shortcuts">
                            <span>Press</span>
                            <kbd class="px-1 py-0.5 bg-gray-700 rounded text-xs">?</kbd>
                            <span>for shortcuts</span>
                        </button>
                    </div>
                    <div class="flex space-x-3">
                        <button
                            id="bulk-rescan-btn"
                            onclick="batchSelection.rescanSelected()"
                            aria-label="Rescan selected files now"
                            class="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded transition focus:outline-none focus:ring-2 focus:ring-blue-500 flex items-center space-x-2">
                            <span class="bulk-rescan-icon"></span>
                            <span>Rescan Selected</span>
                        </button>
                        <button
                            id="bulk-delete-btn"
                            onclick="batchSelection.deleteSelected()"
                            aria-label="Delete selected files"
                            class="px-4 py-2 bg-red-600 hover:bg-red-700 rounded transition focus:outline-none focus:ring-2 focus:ring-red-500 flex items-center space-x-2">
                            <span class="bulk-delete-icon"></span>
                            <span>Delete Selected</span>
                        </button>
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(toolbar);

        // Inject icons after toolbar is in DOM
        if (window.Icons) {
            const rescanIcon = toolbar.querySelector('.bulk-rescan-icon');
            const deleteIcon = toolbar.querySelector('.bulk-delete-icon');
            if (rescanIcon) rescanIcon.innerHTML = Icons.get('refresh', 5);
            if (deleteIcon) deleteIcon.innerHTML = Icons.get('trash', 5);
        }
    }

    updateUI() {
        // Update select all checkbox state
        const selectAll = document.getElementById('select-all');
        const checkboxes = document.querySelectorAll('.file-checkbox');

        if (selectAll && checkboxes.length > 0) {
            const checkedCount = Array.from(checkboxes).filter(cb => cb.checked).length;
            selectAll.checked = checkedCount === checkboxes.length;
            selectAll.indeterminate = checkedCount > 0 && checkedCount < checkboxes.length;
        }

        // Update toolbar visibility and count
        const toolbar = document.getElementById('bulk-actions-toolbar');
        const count = this.selectedFiles.size;

        if (toolbar) {
            const countEl = toolbar.querySelector('#selected-count');
            if (countEl) {
                countEl.textContent = count;
            }

            if (count > 0) {
                toolbar.style.translate = '0 0';
            } else {
                toolbar.style.translate = '0 100%';
            }
        }
    }

    clearSelection() {
        this.selectedFiles.clear();
        document.querySelectorAll('.file-checkbox').forEach(cb => {
            cb.checked = false;
        });
        this.updateUI();
    }

    async rescanSelected() {
        if (this.selectedFiles.size === 0) return;

        const confirmed = await window.confirmDialog(
            `Rescan ${this.selectedFiles.size} selected file(s) now?`,
            'Rescan Files'
        );

        if (!confirmed) {
            return;
        }

        // Show loading state and disable buttons
        this.setLoadingState(true, 'Rescanning files...');
        this.setButtonsDisabled(true);
        window.showToast && window.showToast('Rescanning files...', 'info');

        try {
            // Send all file IDs in a single bulk request
            const fileIds = Array.from(this.selectedFiles);
            const idsParam = fileIds.join(',');

            const response = await fetch(`/api/files/rescan?ids=${encodeURIComponent(idsParam)}`, {
                method: 'POST'
            });

            if (!response.ok) {
                throw new Error('Rescan request failed');
            }

            window.showToast && window.showToast(`Rescanning ${this.selectedFiles.size} file(s)...`, 'success');
            this.clearSelection();
        } catch (error) {
            window.showToast && window.showToast('Failed to start rescan', 'error');
        } finally {
            this.setLoadingState(false);
            this.setButtonsDisabled(false);
        }
    }

    async deleteSelected() {
        if (this.selectedFiles.size === 0) return;

        // Check if filesystem deletion is enabled (from global config)
        const deleteFromFilesystem = window.appConfig?.deleteFilesFromFilesystem || false;

        // Build appropriate confirmation message
        let confirmMessage;
        let confirmTitle;
        let confirmType = 'confirm';

        if (deleteFromFilesystem) {
            confirmMessage = `<strong>Warning:</strong> You are about to permanently delete <strong>${this.selectedFiles.size} files</strong> from the filesystem.\n\nThis will remove the actual files from disk and <strong>cannot be undone</strong>.\n\nAre you absolutely sure?`;
            confirmTitle = 'Delete Files From Filesystem';
            confirmType = 'warning';
        } else {
            confirmMessage = `You are about to remove ${this.selectedFiles.size} files from the database.\n\nThe actual files will remain on disk. You can re-scan to add them back.\n\nContinue?`;
            confirmTitle = 'Remove From Database';
        }

        const confirmed = await window.confirmDialog(confirmMessage, confirmTitle, confirmType);

        if (!confirmed) {
            return;
        }

        // Show loading state and disable buttons
        const loadingMsg = deleteFromFilesystem ? 'Deleting files from filesystem...' : 'Removing files from database...';
        this.setLoadingState(true, loadingMsg);
        this.setButtonsDisabled(true);
        window.showToast && window.showToast(loadingMsg, 'info');

        try {
            // Use true batch mode - single HTTP request with all file IDs
            // Convert string IDs to numbers (Go expects int64)
            const fileIds = Array.from(this.selectedFiles).map(id => parseInt(id, 10));

            const response = await fetch('/api/files/batch-delete', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ file_ids: fileIds })
            });

            if (!response.ok) {
                throw new Error(`Batch delete request failed: ${response.status}`);
            }

            const result = await response.json();

            // Remove successfully deleted rows from UI
            if (result.results) {
                result.results.forEach(fileResult => {
                    if (fileResult.success) {
                        const checkbox = document.querySelector(`.file-checkbox[data-file-id="${fileResult.file_id}"]`);
                        if (checkbox) {
                            const row = checkbox.closest('tr');
                            if (row) row.remove();
                        }
                    }
                });
            }

            // Show appropriate success message
            const successMsg = deleteFromFilesystem
                ? `Deleted ${result.deleted} files from filesystem`
                : `Removed ${result.deleted} files from database`;

            // Include failure info if any
            if (result.failed > 0) {
                const failureMsg = `${result.failed} files failed to delete`;
                window.showToast && window.showToast(`${successMsg}. ${failureMsg}`, 'warning');
                // Log detailed errors for debugging
                if (result.results) {
                    result.results.forEach(fileResult => {
                        if (!fileResult.success) {
                            console.error(`Failed to delete file ${fileResult.file_id}: ${fileResult.error}`);
                        }
                    });
                }
            } else {
                window.showToast && window.showToast(successMsg, 'success');
            }

            this.clearSelection();
        } catch (error) {
            const errorMsg = deleteFromFilesystem
                ? 'Failed to delete files from filesystem'
                : 'Failed to remove files from database';
            window.showToast && window.showToast(errorMsg, 'error');
            console.error('Batch delete error:', error);
        } finally {
            this.setLoadingState(false);
            this.setButtonsDisabled(false);
        }
    }

    /**
     * Set loading state with optional message
     * @param {boolean} loading - Whether to show loading state
     * @param {string} message - Optional loading message
     */
    setLoadingState(loading, message = 'Processing...') {
        const toolbar = document.getElementById('bulk-actions-toolbar');
        if (!toolbar) return;

        let spinner = toolbar.querySelector('.loading-spinner');

        if (loading) {
            if (!spinner) {
                spinner = document.createElement('div');
                spinner.className = 'loading-spinner flex items-center space-x-2 text-sm text-blue-400';
                spinner.innerHTML = `
                    ${Icons.get('spinner', 4)}
                    <span>${message}</span>
                `;
                toolbar.querySelector('.flex').appendChild(spinner);
            } else {
                spinner.querySelector('span').textContent = message;
            }
        } else if (spinner) {
            spinner.remove();
        }
    }

    /**
     * Execute an async operation on items in batches with controlled concurrency
     * Includes retry logic with exponential backoff for failed operations
     * @param {Array} items - Array of items to process
     * @param {Function} operation - Async function to execute for each item
     * @param {number} concurrency - Maximum number of concurrent operations
     * @param {number} maxRetries - Maximum number of retries per item
     */
    async batchOperation(items, operation, concurrency = 10, maxRetries = 3) {
        const results = [];
        const executing = [];
        let failed = 0;

        const executeWithRetry = async (item, retries = 0) => {
            try {
                return await operation(item);
            } catch (error) {
                if (retries < maxRetries) {
                    // Exponential backoff: 100ms, 200ms, 400ms, etc.
                    const delay = Math.min(100 * Math.pow(2, retries), 2000);
                    await new Promise(resolve => setTimeout(resolve, delay));
                    return executeWithRetry(item, retries + 1);
                }
                failed++;
                throw error;
            }
        };

        for (const item of items) {
            const promise = executeWithRetry(item).then(result => {
                executing.splice(executing.indexOf(promise), 1);
                return result;
            }).catch(err => {
                executing.splice(executing.indexOf(promise), 1);
                // Don't throw, just log
                console.error('Operation failed after retries:', err);
            });

            results.push(promise);
            executing.push(promise);

            if (executing.length >= concurrency) {
                await Promise.race(executing);
            }
        }

        // Wait for all remaining operations to complete
        await Promise.all(results);

        if (failed > 0) {
            window.showToast && window.showToast(`${failed} operations failed`, 'warning');
        }
    }

    setButtonsDisabled(disabled) {
        const toolbar = document.getElementById('bulk-actions-toolbar');
        if (toolbar) {
            const buttons = toolbar.querySelectorAll('button');
            buttons.forEach(btn => {
                btn.disabled = disabled;
                if (disabled) {
                    btn.classList.add('opacity-50', 'cursor-not-allowed');
                } else {
                    btn.classList.remove('opacity-50', 'cursor-not-allowed');
                }
            });
        }
    }

    showKeyboardShortcutsHelp() {
        const shortcuts = [
            { key: 'Ctrl/Cmd + A', description: 'Select all files on current page' },
            { key: 'Shift + Click', description: 'Select range of files' },
            { key: 'Escape', description: 'Clear current selection' },
            { key: '?', description: 'Show this help dialog' }
        ];

        const shortcutsList = shortcuts
            .map(s => `<div class="flex justify-between py-2 border-b border-gray-700 last:border-0">
                <kbd class="px-2 py-1 bg-gray-700 rounded text-sm font-mono">${s.key}</kbd>
                <span class="text-gray-300">${s.description}</span>
            </div>`)
            .join('');

        if (window.modalManager) {
            window.modalManager.alert(
                `<div class="space-y-2">${shortcutsList}</div>`,
                'Keyboard Shortcuts',
                'info'
            );
        }
    }
}

// Initialize batch selection
const batchSelection = new BatchSelection();

// Expose globally
window.batchSelection = batchSelection;

