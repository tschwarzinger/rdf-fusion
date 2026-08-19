<script>
    import { purgeAllDatabaseAndStorage } from '../db.js';

    let isClearing = $state(false);

    async function handleConfirmPurge() {
        isClearing = true;
        try {
            await purgeAllDatabaseAndStorage();
        } catch (e) {
            console.error("Error during purge:", e);
        } finally {
            // Reload the page to ensure complete fresh start
            window.location.reload();
        }
    }
</script>

<div class="modal fade" id="clearAllModal" tabindex="-1" aria-labelledby="clearAllModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-dialog-scrollable">
        <div class="modal-content shadow-lg border-0">
            <div class="modal-header bg-danger-subtle border-bottom border-danger-subtle py-3">
                <h5 class="modal-title d-flex align-items-center gap-2 text-danger fw-bold fs-6" id="clearAllModalLabel">
                    <i class="fa-solid fa-triangle-exclamation"></i>
                    Clear All Playground Data
                </h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close" disabled={isClearing}></button>
            </div>
            <div class="modal-body p-4">
                <p class="text-secondary mb-3">
                    Are you sure you want to purge all playground data? This will reset the playground back to its initial state.
                </p>
                <div class="card bg-light border-0 rounded-3 p-3 mb-3">
                    <div class="fw-semibold small mb-2 text-dark">The following data will be permanently removed:</div>
                    <ul class="list-unstyled mb-0 small text-secondary d-flex flex-column gap-2">
                        <li class="d-flex align-items-start gap-2">
                            <i class="fa-solid fa-database text-danger mt-1"></i>
                            <div><strong>Datasets:</strong> All local datasets, uploaded datasets, and stored blobs.</div>
                        </li>
                        <li class="d-flex align-items-start gap-2">
                            <i class="fa-solid fa-microchip text-danger mt-1"></i>
                            <div><strong>Engines:</strong> All downloaded builds and uploaded engine binaries.</div>
                        </li>
                        <li class="d-flex align-items-start gap-2">
                            <i class="fa-solid fa-sliders text-danger mt-1"></i>
                            <div><strong>Settings:</strong> Custom engine settings.</div>
                        </li>
                    </ul>
                </div>
            </div>
            <div class="modal-footer bg-light border-top py-2 d-flex justify-content-end gap-2">
                <button type="button" class="btn btn-sm btn-outline-secondary" data-bs-dismiss="modal" disabled={isClearing}>
                    Cancel
                </button>
                <button type="button" class="btn btn-sm btn-danger d-flex align-items-center gap-2" onclick={handleConfirmPurge} disabled={isClearing}>
                    {#if isClearing}
                        <i class="fa-solid fa-spinner fa-spin"></i>
                        <span>Clearing Data...</span>
                    {:else}
                        <i class="fa-solid fa-trash-can"></i>
                        <span>Clear Everything & Reload</span>
                    {/if}
                </button>
            </div>
        </div>
    </div>
</div>
