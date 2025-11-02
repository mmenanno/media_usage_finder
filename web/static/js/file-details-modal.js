// File Details Modal System

class FileDetailsModal {
    constructor() {
        this.createModalContainer();
        this.setupEventListeners();
    }

    createModalContainer() {
        const container = document.createElement('div');
        container.id = 'file-details-modal';
        container.className = 'fixed inset-0 z-50 hidden';
        document.body.appendChild(container);
    }

    setupEventListeners() {
        // Listen for custom event to show file details
        document.addEventListener('showFileDetails', (event) => {
            this.show(event.detail.fileId);
        });

        // Handle clicks on details buttons using event delegation
        document.addEventListener('click', (event) => {
            const button = event.target.closest('[data-action="show-details"]');
            if (button) {
                const fileId = button.dataset.fileId;
                if (fileId) {
                    this.show(fileId);
                }
            }
        });

        // Close on escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !document.getElementById('file-details-modal').classList.contains('hidden')) {
                this.hide();
            }
        });
    }

    async show(fileId) {
        try {
            // Fetch file details
            const response = await fetch(`/api/files/${fileId}/details`);
            if (!response.ok) {
                throw new Error('Failed to fetch file details');
            }

            const data = await response.json();
            this.render(data);
        } catch (error) {
            console.error('Error loading file details:', error);
            window.showToast('Failed to load file details', 'error');
        }
    }

    render(fileData) {
        const modal = document.getElementById('file-details-modal');

        // Build usage badges
        const usageBadges = fileData.usage && fileData.usage.length > 0
            ? fileData.usage.map(u => `
                <span class="px-2 py-1 bg-${this.getServiceColor(u.service)}-600 rounded text-xs capitalize">
                    ${u.service}
                </span>
              `).join('')
            : '<span class="text-gray-500 text-sm">Not tracked by any service</span>';

        // Build metadata sections
        const metadataSections = fileData.usage && fileData.usage.length > 0
            ? fileData.usage.map(u => `
                <div class="border-t border-gray-700 pt-4">
                    <h4 class="text-sm font-medium text-gray-400 mb-2 capitalize">${u.service} Metadata</h4>
                    <div class="space-y-1 text-sm">
                        ${this.renderMetadata(u.metadata)}
                    </div>
                </div>
              `).join('')
            : '';

        // Build hardlink info
        const hardlinkInfo = fileData.hardlinks && fileData.hardlinks.length > 1
            ? `
                <div class="border-t border-gray-700 pt-4">
                    <h4 class="text-sm font-medium text-gray-400 mb-2">Hardlinks (${fileData.hardlinks.length} total)</h4>
                    <div class="space-y-2 max-h-48 overflow-y-auto">
                        ${fileData.hardlinks.map(path => `
                            <div class="text-sm font-mono text-gray-300 p-2 bg-gray-700 rounded break-all">
                                ${path}
                            </div>
                        `).join('')}
                    </div>
                </div>
              `
            : '';

        modal.innerHTML = `
            <div class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 animate-fadeIn" onclick="fileDetailsModal.hide()">
                <div class="bg-gray-800 rounded-lg shadow-xl max-w-3xl w-full max-h-[90vh] overflow-hidden transform transition-all animate-scaleIn" onclick="event.stopPropagation()">
                    <!-- Header -->
                    <div class="bg-gray-700 px-6 py-4 flex justify-between items-start border-b border-gray-600">
                        <div class="flex-1 pr-4">
                            <h3 class="text-lg font-semibold text-white mb-1">File Details</h3>
                            <p class="text-sm font-mono text-gray-300 break-all">${fileData.path}</p>
                        </div>
                        <button
                            onclick="fileDetailsModal.hide()"
                            class="text-gray-400 hover:text-white transition p-1"
                            aria-label="Close">
                            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                            </svg>
                        </button>
                    </div>

                    <!-- Content -->
                    <div class="p-6 overflow-y-auto max-h-[calc(90vh-200px)]">
                        <!-- Basic Info -->
                        <div class="grid grid-cols-2 gap-4 mb-6">
                            <div>
                                <label class="text-xs text-gray-500 uppercase">Size</label>
                                <p class="text-white font-medium">${this.formatSize(fileData.size)}</p>
                            </div>
                            <div>
                                <label class="text-xs text-gray-500 uppercase">Modified</label>
                                <p class="text-white font-medium">${this.formatDate(fileData.modified_time)}</p>
                            </div>
                            <div>
                                <label class="text-xs text-gray-500 uppercase">Inode</label>
                                <p class="text-white font-mono">${fileData.inode}</p>
                            </div>
                            <div>
                                <label class="text-xs text-gray-500 uppercase">Device ID</label>
                                <p class="text-white font-mono">${fileData.device_id}</p>
                            </div>
                            <div>
                                <label class="text-xs text-gray-500 uppercase">Status</label>
                                <p class="text-white">
                                    ${fileData.is_orphaned
                                        ? '<span class="px-2 py-1 bg-yellow-600 rounded text-xs">Orphaned</span>'
                                        : '<span class="px-2 py-1 bg-green-600 rounded text-xs">In Use</span>'}
                                </p>
                            </div>
                            <div>
                                <label class="text-xs text-gray-500 uppercase">Last Verified</label>
                                <p class="text-white font-medium">${this.formatDate(fileData.last_verified)}</p>
                            </div>
                        </div>

                        <!-- Services -->
                        <div class="mb-6">
                            <label class="text-xs text-gray-500 uppercase mb-2 block">Services</label>
                            <div class="flex flex-wrap gap-2">
                                ${usageBadges}
                            </div>
                        </div>

                        ${metadataSections}
                        ${hardlinkInfo}
                    </div>

                    <!-- Footer Actions -->
                    <div class="bg-gray-700 px-6 py-4 flex justify-between items-center border-t border-gray-600">
                        <div class="flex space-x-2">
                            <button
                                hx-post="/api/files/mark-rescan?id=${fileData.id}"
                                class="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded transition cursor-pointer">
                                Mark for Rescan
                            </button>
                            <button
                                hx-delete="/api/files/delete?id=${fileData.id}"
                                hx-confirm="Are you sure you want to delete this file? This action cannot be undone."
                                class="px-4 py-2 bg-red-600 hover:bg-red-700 rounded transition cursor-pointer">
                                Delete File
                            </button>
                        </div>
                        <button
                            onclick="fileDetailsModal.hide()"
                            class="px-4 py-2 bg-gray-600 hover:bg-gray-500 rounded transition cursor-pointer">
                            Close
                        </button>
                    </div>
                </div>
            </div>
        `;

        modal.classList.remove('hidden');

        // Re-initialize HTMX for new buttons
        if (window.htmx) {
            htmx.process(modal);
        }
    }

    hide() {
        const modal = document.getElementById('file-details-modal');
        modal.classList.add('hidden');
        modal.innerHTML = '';
    }

    renderMetadata(metadata) {
        if (!metadata || Object.keys(metadata).length === 0) {
            return '<span class="text-gray-500">No metadata available</span>';
        }

        return Object.entries(metadata).map(([key, value]) => `
            <div class="flex justify-between">
                <span class="text-gray-400 capitalize">${key.replace(/_/g, ' ')}:</span>
                <span class="text-gray-200">${value}</span>
            </div>
        `).join('');
    }

    getServiceColor(service) {
        const colors = {
            plex: 'blue',
            sonarr: 'purple',
            radarr: 'yellow',
            qbittorrent: 'green'
        };
        return colors[service] || 'gray';
    }

    formatSize(bytes) {
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let size = bytes;
        let unitIndex = 0;

        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }

        return `${size.toFixed(2)} ${units[unitIndex]}`;
    }

    formatDate(timestamp) {
        const date = new Date(timestamp * 1000);
        return date.toLocaleString();
    }
}

// Initialize modal manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.fileDetailsModal = new FileDetailsModal();
});

// Helper function to show file details
window.showFileDetails = (fileId) => {
    document.dispatchEvent(new CustomEvent('showFileDetails', {
        detail: { fileId }
    }));
};

