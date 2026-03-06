'use client';

import { Progress } from '@/components/ui';
import { useState, useEffect } from 'react';

export default function ProgressDemo() {
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => {
      setProgress((prev) => (prev + 5) % 105); // Cycle from 0 to 100
    }, 1000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="min-h-screen bg-background p-8">
      <div className="max-w-4xl mx-auto space-y-8">
        <div className="text-center">
          <h1 className="text-3xl font-bold mb-2">Progress Bar Component Demo</h1>
          <p className="text-muted-foreground">Reusable progress bar for exam progress, profile completion, etc.</p>
        </div>

        {/* Animated Progress Demo */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">Animated Progress (Current: {progress}%)</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-2">
              <p className="text-sm font-medium">Primary Color</p>
              <Progress value={progress} color="primary" />
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">With Label</p>
              <Progress value={progress} showLabel color="primary" />
            </div>
          </div>
        </div>

        {/* Size Variants */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">Size Variants</h2>
          <div className="space-y-4">
            <div className="space-y-2">
              <p className="text-sm font-medium">Small (sm)</p>
              <Progress value={75} size="sm" color="success" />
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Medium (md) - Default</p>
              <Progress value={75} size="md" color="success" />
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Large (lg)</p>
              <Progress value={75} size="lg" color="success" />
            </div>
          </div>
        </div>

        {/* Color Variants */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">Color Variants</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-2">
              <p className="text-sm font-medium">Primary</p>
              <Progress value={85} color="primary" showLabel />
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Success</p>
              <Progress value={85} color="success" showLabel />
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Warning</p>
              <Progress value={85} color="warning" showLabel />
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">Danger</p>
              <Progress value={85} color="danger" showLabel />
            </div>
          </div>
        </div>

        {/* Edge Cases */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">Edge Cases</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="space-y-2">
              <p className="text-sm font-medium">0% (Empty)</p>
              <Progress value={0} showLabel />
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">100% (Full)</p>
              <Progress value={100} showLabel />
            </div>
            <div className="space-y-2">
              <p className="text-sm font-medium">150% (Clamped to 100%)</p>
              <Progress value={150} showLabel />
            </div>
          </div>
        </div>

        {/* Usage Examples */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">Usage Examples</h2>
          <div className="bg-muted p-4 rounded-lg">
            <pre className="text-sm overflow-x-auto">
{`// Basic progress bar
<Progress value={75} />

// With label and custom styling
<Progress 
  value={85} 
  showLabel 
  size="lg" 
  color="success" 
  className="w-64"
/>

// Exam progress
<Progress value={examProgress} color="primary" showLabel />

// Profile completion
<Progress value={profileCompletion} color="success" size="sm" />`}
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}