// Batch Selection System for Files

class BatchSelection {
    constructor() {
        this.selectedFiles = new Set();
        this.lastSelectedIndex = null;
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

            // Add event listener for shift-click range selection
            const checkbox = td.querySelector('.file-checkbox');
            checkbox.addEventListener('change', (e) => {
                this.handleCheckboxChange(e, index);
            });
        });
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
        toolbar.innerHTML = `
            <div class="container mx-auto px-4 py-4">
                <div class="flex items-center justify-between">
                    <div class="flex items-center space-x-4">
                        <span class="text-sm text-gray-300">
                            <span id="selected-count" class="font-bold text-blue-400">0</span> files selected
                        </span>
                        <button
                            onclick="batchSelection.clearSelection()"
                            class="text-sm text-gray-400 hover:text-white transition">
                            Clear selection
                        </button>
                    </div>
                    <div class="flex space-x-3">
                        <button
                            onclick="batchSelection.markSelectedForRescan()"
                            class="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded transition">
                            Mark for Rescan
                        </button>
                        <button
                            onclick="batchSelection.deleteSelected()"
                            class="px-4 py-2 bg-red-600 hover:bg-red-700 rounded transition">
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

        if (!confirm(`Mark ${this.selectedFiles.size} files for rescan?`)) {
            return;
        }

        const promises = Array.from(this.selectedFiles).map(fileId =>
            fetch(`/api/files/mark-rescan?id=${fileId}`, { method: 'POST' })
        );

        try {
            await Promise.all(promises);
            window.showToast && window.showToast(`Marked ${this.selectedFiles.size} files for rescan`, 'success');
            this.clearSelection();
        } catch (error) {
            window.showToast && window.showToast('Failed to mark files for rescan', 'error');
        }
    }

    async deleteSelected() {
        if (this.selectedFiles.size === 0) return;

        if (!confirm(`Are you ABSOLUTELY SURE you want to delete ${this.selectedFiles.size} files? This CANNOT be undone!`)) {
            return;
        }

        const promises = Array.from(this.selectedFiles).map(fileId =>
            fetch(`/api/files/delete?id=${fileId}`, { method: 'DELETE' })
        );

        try {
            await Promise.all(promises);
            window.showToast && window.showToast(`Deleted ${this.selectedFiles.size} files`, 'success');

            // Remove deleted rows from UI
            this.selectedFiles.forEach(fileId => {
                const checkbox = document.querySelector(`.file-checkbox[data-file-id="${fileId}"]`);
                if (checkbox) {
                    const row = checkbox.closest('tr');
                    if (row) row.remove();
                }
            });

            this.clearSelection();
        } catch (error) {
            window.showToast && window.showToast('Failed to delete some files', 'error');
        }
    }
}

// Initialize batch selection
const batchSelection = new BatchSelection();

// Expose globally
window.batchSelection = batchSelection;

