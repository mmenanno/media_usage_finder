// Scan Error Details Modal System

class ScanErrorModal {
    constructor() {
        this.createModalContainer();
        this.setupEventListeners();
    }

    createModalContainer() {
        const container = document.createElement('div');
        container.id = 'scan-error-modal';
        container.className = 'fixed inset-0 z-50 hidden';
        document.body.appendChild(container);
    }

    setupEventListeners() {
        // Handle clicks on error details buttons using event delegation
        document.addEventListener('click', (event) => {
            const button = event.target.closest('[data-action="show-error-details"]');
            if (button) {
                const scanId = button.dataset.scanId;
                const errorDataBase64 = button.dataset.errorsBase64;
                const scanType = button.dataset.scanType;
                const timestamp = button.dataset.timestamp;

                if (scanId && errorDataBase64) {
                    this.show(scanId, errorDataBase64, scanType, timestamp);
                }
            }
        });

        // Close on escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !document.getElementById('scan-error-modal').classList.contains('hidden')) {
                this.hide();
            }
        });
    }

    show(scanId, errorDataBase64, scanType, timestamp) {
        try {
            // Decode base64 data
            let errorDataJson;
            try {
                errorDataJson = atob(errorDataBase64);
            } catch (decodeError) {
                console.error('Failed to decode base64 error data:', decodeError);
                window.showToast('Failed to decode error data', 'error');
                return;
            }

            // Parse error data from JSON
            let errors = [];
            try {
                errors = JSON.parse(errorDataJson);
                // Ensure it's an array
                if (!Array.isArray(errors)) {
                    // If it's a string (legacy format), split by newlines
                    errors = errorDataJson.split('\n').filter(e => e.trim());
                }
            } catch (parseError) {
                // Fallback for non-JSON error strings (legacy format)
                errors = errorDataJson.split('\n').filter(e => e.trim());
            }

            this.render(scanId, errors, scanType, timestamp);
        } catch (error) {
            console.error('Error parsing scan errors:', error);
            window.showToast('Failed to display error details', 'error');
        }
    }

    render(scanId, errors, scanType, timestamp) {
        const modal = document.getElementById('scan-error-modal');

        // Build error list
        const errorList = errors.length > 0
            ? errors.map((error, index) => `
                <div class="p-3 bg-red-950/30 border border-red-800/50 rounded-md hover:bg-red-950/50 transition">
                    <div class="flex items-start gap-2">
                        <span class="text-red-400 text-xs font-mono mt-0.5">#${index + 1}</span>
                        <pre class="flex-1 text-sm text-red-200 whitespace-pre-wrap font-mono wrap-break-word">${this.escapeHtml(error)}</pre>
                    </div>
                </div>
              `).join('')
            : '<p class="text-gray-400 text-center py-8">No errors to display</p>';

        modal.innerHTML = `
            <div class="fixed inset-0 /75 bg-opacity-50 flex items-center justify-center p-4 animate-fadeIn" onclick="scanErrorModal.hide()">
                <div class="bg-gray-800 rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-hidden transform transition-all animate-scaleIn" onclick="event.stopPropagation()">
                    <!-- Header -->
                    <div class="bg-gray-700 px-6 py-4 flex justify-between items-start border-b border-gray-600">
                        <div class="flex-1 pr-4">
                            <h3 class="text-lg font-semibold text-white mb-1">Scan Error Details</h3>
                            <div class="flex items-center gap-4 text-sm text-gray-300">
                                <span>Scan ID: <span class="font-mono">#${scanId}</span></span>
                                ${scanType ? `<span class="px-2 py-0.5 bg-gray-600 rounded text-xs">${this.formatScanType(scanType)}</span>` : ''}
                                ${timestamp ? `<span>${this.formatTimestamp(parseInt(timestamp))}</span>` : ''}
                            </div>
                        </div>
                        <button
                            onclick="scanErrorModal.hide()"
                            class="text-gray-400 hover:text-white transition p-1"
                            aria-label="Close">
                            ${Icons.get('close', 6)}
                        </button>
                    </div>

                    <!-- Content -->
                    <div class="p-6 overflow-y-auto max-h-[calc(90vh-200px)]">
                        <!-- Error Summary -->
                        <div class="mb-4 p-4 bg-red-900/20 border border-red-800/50 rounded-lg">
                            <div class="flex items-center gap-2 text-red-300">
                                ${Icons.get('warning', 5)}
                                <span class="font-medium">Total Errors: ${errors.length}</span>
                            </div>
                        </div>

                        <!-- Error List -->
                        <div class="space-y-2">
                            ${errorList}
                        </div>
                    </div>

                    <!-- Footer -->
                    <div class="bg-gray-700 px-6 py-4 flex justify-end items-center border-t border-gray-600">
                        <button
                            onclick="scanErrorModal.hide()"
                            class="px-4 py-2 bg-gray-600 hover:bg-gray-500 rounded transition">
                            Close
                        </button>
                    </div>
                </div>
            </div>
        `;

        modal.classList.remove('hidden');
    }

    hide() {
        const modal = document.getElementById('scan-error-modal');
        modal.classList.add('hidden');
        modal.innerHTML = '';
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    formatScanType(type) {
        const typeMap = {
            'full': 'Full Scan',
            'incremental': 'Incremental Scan',
            'hash_scan': 'Hash Scan',
            'service_update_all': 'Update All Services',
            'service_update_plex': 'Update Plex',
            'service_update_sonarr': 'Update Sonarr',
            'service_update_radarr': 'Update Radarr',
            'service_update_qbittorrent': 'Update qBittorrent',
            'service_update_stash': 'Update Stash',
            'disk_location': 'Disk Location Scan'
        };
        return typeMap[type] || type;
    }

    formatTimestamp(timestamp) {
        const date = new Date(timestamp * 1000);
        return date.toLocaleString();
    }
}

// Initialize modal manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.scanErrorModal = new ScanErrorModal();
});

// Helper function to show scan error details
window.showScanErrorDetails = (scanId, errorData, scanType, timestamp) => {
    if (window.scanErrorModal) {
        window.scanErrorModal.show(scanId, errorData, scanType, timestamp);
    }
};
