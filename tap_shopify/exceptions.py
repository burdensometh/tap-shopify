class ShopifyError(Exception):
    def __init__(self, error, msg=''):
        super().__init__('{}\n{}'.format(error.__class__.__name__, msg))

class ShopifyUnauthorizedError(Exception):
    def __init__(self, error, msg=''):
        super().__init__('{}\n{}'.format(error.__class__.__name__, msg))

class ShopifyAPIError(Exception):
    """Raised for any unexpected api error without a valid status code"""

class BulkWindowConflict(ShopifyAPIError):
    """Raised when the parts of a bulk window disagree and the window must be redone.

    The parts of a window are separate bulk operations and therefore separate
    snapshots.  A root that changed between them (different ``updatedAt``,
    conflicting shared fields, a repeated part or a changed root set) cannot be
    delivered as a single consistent record; the whole window has to be
    re-read instead of silently overwriting the conflicting fields.
    """

class BulkOperationInProgressError(Exception):
    """Raised when a bulk operation is already in progress"""
    def __init__(self, message, bulk_op_id=None):
        super().__init__(message)
        self.bulk_op_id = bulk_op_id
