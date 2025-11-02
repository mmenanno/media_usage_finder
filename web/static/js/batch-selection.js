// Batch Selection System for Files

class BatchSelection {
    constructor() {
        this.selectedFiles = new Set();
        this.lastSelectedIndex = null;
        this.tableChangeHandler = null; // Store handler for cleanup
        this.setupEventListeners();
    }

    setupEventListeners() {
        document.addEventListener('DOMContentLoaded', () => {
            this.init();
        });

        // Listen for table updates (HTMX swaps)
        document.body.addEventListener('htmx:afterSwap', (event) => {
            if (event.detail.target.id === 'files-table') {
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
                class="w-4 h-4 bg-gray-700 border-gray-600 rounded cursor-pointer">
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
                    class="file-checkbox w-4 h-4 bg-gray-700 border-gray-600 rounded cursor-pointer"
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
                            onclick="batchSelection.markSelectedForRescan()"
                            aria-label="Mark selected files for rescan"
                            class="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded transition focus:outline-none focus:ring-2 focus:ring-blue-500">
                            Mark for Rescan
                        </button>
                        <button
                            onclick="batchSelection.deleteSelected()"
                            aria-label="Delete selected files"
                            class="px-4 py-2 bg-red-600 hover:bg-red-700 rounded transition focus:outline-none focus:ring-2 focus:ring-red-500">
                            Delete Selected
                        </button>
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(toolbar);
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
                toolbar.style.transform = 'translateY(0)';
            } else {
                toolbar.style.transform = 'translateY(100%)';
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

    async markSelectedForRescan() {
        if (this.selectedFiles.size === 0) return;

        const confirmed = await window.showConfirm(
            `Mark ${this.selectedFiles.size} files for rescan?`,
            'Mark for Rescan'
        );

        if (!confirmed) {
            return;
        }

        // Show loading state and disable buttons
        this.setLoadingState(true, 'Marking files for rescan...');
        this.setButtonsDisabled(true);
        window.showToast && window.showToast('Marking files for rescan...', 'info');

        try {
            // Use batched concurrent requests to avoid overwhelming the server
            const fileIds = Array.from(this.selectedFiles);
            await this.batchOperation(fileIds, async (fileId) => {
                const response = await fetch(`/api/files/mark-rescan?id=${fileId}`, { method: 'POST' });
                if (!response.ok) throw new Error(`Failed for file ${fileId}`);
            }, 10); // Process 10 at a time

            window.showToast && window.showToast(`Marked ${this.selectedFiles.size} files for rescan`, 'success');
            this.clearSelection();
        } catch (error) {
            window.showToast && window.showToast('Failed to mark some files for rescan', 'error');
        } finally {
            this.setLoadingState(false);
            this.setButtonsDisabled(false);
        }
    }

    async deleteSelected() {
        if (this.selectedFiles.size === 0) return;

        const confirmed = await window.showConfirm(
            `Are you ABSOLUTELY SURE you want to delete ${this.selectedFiles.size} files? This CANNOT be undone!`,
            'Delete Files'
        );

        if (!confirmed) {
            return;
        }

        // Show loading state and disable buttons
        this.setLoadingState(true, 'Deleting files...');
        this.setButtonsDisabled(true);
        window.showToast && window.showToast('Deleting files...', 'info');

        try {
            // Use batched concurrent requests to avoid overwhelming the server
            const fileIds = Array.from(this.selectedFiles);
            let deletedCount = 0;

            await this.batchOperation(fileIds, async (fileId) => {
                const response = await fetch(`/api/files/delete?id=${fileId}`, { method: 'DELETE' });
                if (!response.ok) throw new Error(`Failed for file ${fileId}`);

                // Remove deleted row from UI
                const checkbox = document.querySelector(`.file-checkbox[data-file-id="${fileId}"]`);
                if (checkbox) {
                    const row = checkbox.closest('tr');
                    if (row) row.remove();
                }
                deletedCount++;
            }, 10); // Process 10 at a time

            window.showToast && window.showToast(`Deleted ${deletedCount} files`, 'success');
            this.clearSelection();
        } catch (error) {
            window.showToast && window.showToast('Failed to delete some files', 'error');
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
                    <svg class="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
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

