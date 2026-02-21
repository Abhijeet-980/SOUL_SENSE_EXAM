'use client';

import { Modal } from '@/components/ui';
import { Button } from '@/components/ui';
import { useState } from 'react';

export default function ModalDemo() {
  const [basicModal, setBasicModal] = useState(false);
  const [confirmModal, setConfirmModal] = useState(false);
  const [formModal, setFormModal] = useState(false);
  const [largeModal, setLargeModal] = useState(false);
  const [fullModal, setFullModal] = useState(false);

  return (
    <div className="min-h-screen bg-background p-8">
      <div className="max-w-4xl mx-auto space-y-8">
        <div className="text-center">
          <h1 className="text-3xl font-bold mb-2">Modal Component Demo</h1>
          <p className="text-muted-foreground">Accessible modal dialog for confirmations, forms, and detailed views.</p>
        </div>

        {/* Modal Trigger Buttons */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          <Button onClick={() => setBasicModal(true)} className="h-12">
            Basic Modal
          </Button>
          <Button onClick={() => setConfirmModal(true)} variant="destructive" className="h-12">
            Confirmation Modal
          </Button>
          <Button onClick={() => setFormModal(true)} variant="secondary" className="h-12">
            Form Modal
          </Button>
          <Button onClick={() => setLargeModal(true)} variant="outline" className="h-12">
            Large Modal
          </Button>
          <Button onClick={() => setFullModal(true)} variant="outline" className="h-12">
            Full Width Modal
          </Button>
        </div>

        {/* Size Variants Demo */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">Size Variants</h2>
          <div className="text-sm text-muted-foreground">
            <p>• <strong>sm:</strong> Small modal (max-w-md)</p>
            <p>• <strong>md:</strong> Medium modal (max-w-lg) - default</p>
            <p>• <strong>lg:</strong> Large modal (max-w-2xl)</p>
            <p>• <strong>full:</strong> Full width modal (max-w-full)</p>
          </div>
        </div>

        {/* Accessibility Features */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">Accessibility Features</h2>
          <div className="bg-muted p-4 rounded-lg">
            <ul className="text-sm space-y-2">
              <li>✅ <strong>Focus Trap:</strong> Tab navigation stays within modal</li>
              <li>✅ <strong>Escape Key:</strong> Closes modal when pressed</li>
              <li>✅ <strong>Backdrop Click:</strong> Closes modal when clicking outside</li>
              <li>✅ <strong>Body Scroll Lock:</strong> Prevents background scrolling</li>
              <li>✅ <strong>ARIA Attributes:</strong> Proper dialog role and labels</li>
              <li>✅ <strong>Focus Management:</strong> Restores focus when closed</li>
              <li>✅ <strong>Screen Reader:</strong> Announces modal content</li>
            </ul>
          </div>
        </div>

        {/* Usage Examples */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">Usage Examples</h2>
          <div className="bg-muted p-4 rounded-lg">
            <pre className="text-sm overflow-x-auto">
{`// Basic modal
<Modal
  isOpen={isOpen}
  onClose={() => setIsOpen(false)}
  title="Modal Title"
>
  <p>Modal content goes here.</p>
</Modal>

// Confirmation modal
<Modal
  isOpen={showConfirm}
  onClose={() => setShowConfirm(false)}
  title="Confirm Action"
  size="sm"
>
  <p>Are you sure you want to proceed?</p>
  <div className="flex justify-end gap-2 mt-4">
    <Button variant="outline" onClick={() => setShowConfirm(false)}>
      Cancel
    </Button>
    <Button onClick={handleConfirm}>
      Confirm
    </Button>
  </div>
</Modal>

// Form modal
<Modal
  isOpen={showForm}
  onClose={() => setShowForm(false)}
  title="Edit Profile"
  size="lg"
>
  <form onSubmit={handleSubmit}>
    {/* Form fields */}
    <div className="flex justify-end gap-2 mt-6">
      <Button type="button" variant="outline" onClick={() => setShowForm(false)}>
        Cancel
      </Button>
      <Button type="submit">
        Save
      </Button>
    </div>
  </form>
</Modal>`}
            </pre>
          </div>
        </div>

        {/* Modal Instances */}
        <Modal
          isOpen={basicModal}
          onClose={() => setBasicModal(false)}
          title="Basic Modal"
          size="sm"
        >
          <p className="text-muted-foreground">
            This is a basic modal with default settings. It includes a title, close button,
            and can be closed by clicking the backdrop or pressing Escape.
          </p>
          <div className="flex justify-end mt-4">
            <Button onClick={() => setBasicModal(false)}>
              Close
            </Button>
          </div>
        </Modal>

        <Modal
          isOpen={confirmModal}
          onClose={() => setConfirmModal(false)}
          title="Confirm Deletion"
          size="sm"
        >
          <div className="space-y-4">
            <p className="text-muted-foreground">
              Are you sure you want to delete this item? This action cannot be undone.
            </p>
            <div className="flex justify-end gap-2">
              <Button
                variant="outline"
                onClick={() => setConfirmModal(false)}
              >
                Cancel
              </Button>
              <Button
                variant="destructive"
                onClick={() => {
                  // Handle delete action
                  setConfirmModal(false);
                }}
              >
                Delete
              </Button>
            </div>
          </div>
        </Modal>

        <Modal
          isOpen={formModal}
          onClose={() => setFormModal(false)}
          title="Edit User Profile"
          size="md"
        >
          <form className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-1">
                Full Name
              </label>
              <input
                type="text"
                className="w-full px-3 py-2 border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="Enter your full name"
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">
                Email
              </label>
              <input
                type="email"
                className="w-full px-3 py-2 border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="Enter your email"
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">
                Bio
              </label>
              <textarea
                rows={3}
                className="w-full px-3 py-2 border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="Tell us about yourself"
              />
            </div>
            <div className="flex justify-end gap-2 pt-4">
              <Button
                type="button"
                variant="outline"
                onClick={() => setFormModal(false)}
              >
                Cancel
              </Button>
              <Button type="submit">
                Save Changes
              </Button>
            </div>
          </form>
        </Modal>

        <Modal
          isOpen={largeModal}
          onClose={() => setLargeModal(false)}
          title="Large Modal with Scrollable Content"
          size="lg"
        >
          <div className="space-y-4">
            <p className="text-muted-foreground">
              This modal demonstrates a larger size with scrollable content.
              When content exceeds the maximum height, it becomes scrollable.
            </p>

            {Array.from({ length: 10 }, (_, i) => (
              <div key={i} className="p-4 bg-muted rounded-lg">
                <h3 className="font-medium mb-2">Section {i + 1}</h3>
                <p className="text-sm text-muted-foreground">
                  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
                  Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
                  Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
                </p>
              </div>
            ))}

            <div className="flex justify-end pt-4">
              <Button onClick={() => setLargeModal(false)}>
                Close
              </Button>
            </div>
          </div>
        </Modal>

        <Modal
          isOpen={fullModal}
          onClose={() => setFullModal(false)}
          title="Full Width Modal"
          size="full"
        >
          <div className="space-y-4">
            <p className="text-muted-foreground">
              This modal uses the full width of the screen (with small margins).
              It&apos;s useful for complex forms or detailed views that need maximum space.
            </p>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-4">
                <h3 className="font-medium">Left Column</h3>
                <div className="h-32 bg-muted rounded-lg flex items-center justify-center">
                  Content Area 1
                </div>
                <div className="h-32 bg-muted rounded-lg flex items-center justify-center">
                  Content Area 2
                </div>
              </div>

              <div className="space-y-4">
                <h3 className="font-medium">Right Column</h3>
                <div className="h-32 bg-muted rounded-lg flex items-center justify-center">
                  Content Area 3
                </div>
                <div className="h-32 bg-muted rounded-lg flex items-center justify-center">
                  Content Area 4
                </div>
              </div>
            </div>

            <div className="flex justify-end pt-4">
              <Button onClick={() => setFullModal(false)}>
                Close
              </Button>
            </div>
          </div>
        </Modal>
      </div>
    </div>
  );
}