let userToDelete = null;

function deleteUser(userId) {
    userToDelete = userId;
    const modal = new bootstrap.Modal(document.getElementById('deleteModal'));
    modal.show();
}

document.getElementById('confirmDelete').addEventListener('click', function() {
    if (userToDelete) {
        fetch(`/delete_user/${userToDelete}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // Remove the user row from the table
                const userRow = document.getElementById(`user-${userToDelete}`);
                if (userRow) {
                    userRow.remove();
                }
                
                // Show success message
                showAlert('User deleted successfully!', 'success');
                
                // Check if table is now empty
                const tbody = document.querySelector('tbody');
                if (tbody && tbody.children.length === 0) {
                    location.reload(); // Reload to show empty state
                }
            } else {
                showAlert('Error deleting user: ' + data.message, 'danger');
            }
        })
        .catch(error => {
            showAlert('Error deleting user: ' + error.message, 'danger');
        })
        .finally(() => {
            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('deleteModal'));
            modal.hide();
            userToDelete = null;
        });
    }
});

function showAlert(message, type) {
    // Create alert element
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
    alertDiv.innerHTML = `
        <i class="fas fa-${type === 'success' ? 'check-circle' : 'exclamation-triangle'} me-2"></i>
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    // Insert at the top of the container
    const container = document.querySelector('.container');
    container.insertBefore(alertDiv, container.firstChild);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        if (alertDiv.parentNode) {
            alertDiv.remove();
        }
    }, 5000);
}
