'use client';

import { Button } from '@/components/ui';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui';
import { Heart, Github, ExternalLink, Info, Code, Users } from 'lucide-react';

export function AboutSettings() {
  const version = '1.0.0'; // TODO: Get from package.json or API
  const buildDate = new Date().toLocaleDateString();

  const handleOpenGitHub = () => {
    window.open('https://github.com/your-org/soul-sense-exam', '_blank');
  };

  const handleOpenDocs = () => {
    window.open('/docs', '_blank');
  };

  const handleContactSupport = () => {
    window.open('mailto:support@soulsense.com', '_blank');
  };

  return (
    <div className="space-y-6">
      {/* App Information */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Info className="h-5 w-5" />
            Soul Sense Exam
          </CardTitle>
          <CardDescription>
            An emotional intelligence assessment platform
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <p className="font-medium">Version</p>
              <p className="text-muted-foreground">{version}</p>
            </div>
            <div>
              <p className="font-medium">Build Date</p>
              <p className="text-muted-foreground">{buildDate}</p>
            </div>
          </div>

          <div className="pt-4 space-y-2">
            <Button variant="outline" onClick={handleOpenDocs} className="w-full justify-start">
              <ExternalLink className="h-4 w-4 mr-2" />
              View Documentation
            </Button>
            <Button variant="outline" onClick={handleOpenGitHub} className="w-full justify-start">
              <Github className="h-4 w-4 mr-2" />
              View on GitHub
            </Button>
            <Button variant="outline" onClick={handleContactSupport} className="w-full justify-start">
              <Heart className="h-4 w-4 mr-2" />
              Contact Support
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Credits */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Users className="h-5 w-5" />
            Credits
          </CardTitle>
          <CardDescription>
            People who made this possible
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div>
            <h4 className="font-medium mb-2">Development Team</h4>
            <ul className="text-sm text-muted-foreground space-y-1">
              <li>• Frontend: React & TypeScript</li>
              <li>• Backend: Python & FastAPI</li>
              <li>• Database: SQLite with SQLAlchemy</li>
              <li>• ML: Scikit-learn & TensorFlow</li>
            </ul>
          </div>

          <div>
            <h4 className="font-medium mb-2">Open Source Libraries</h4>
            <p className="text-sm text-muted-foreground">
              This application uses several open source libraries including React, Next.js,
              Tailwind CSS, Lucide React, and many others. We are grateful to the open source community.
            </p>
          </div>
        </CardContent>
      </Card>

      {/* License */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Code className="h-5 w-5" />
            License
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Soul Sense Exam is licensed under the MIT License. This means you are free to use,
            modify, and distribute this software, subject to the terms of the license.
          </p>
          <Button variant="link" className="p-0 h-auto mt-2" onClick={() => window.open('/license', '_blank')}>
            View full license
          </Button>
        </CardContent>
      </Card>

      {/* Support */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Heart className="h-5 w-5" />
            Support the Project
          </CardTitle>
          <CardDescription>
            Help us improve Soul Sense Exam
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-sm text-muted-foreground">
            Soul Sense Exam is a free and open source project. If you find it helpful,
            consider supporting us by:
          </p>
          <ul className="text-sm text-muted-foreground space-y-1 ml-4">
            <li>• ⭐ Starring the repository on GitHub</li>
            <li>• 🐛 Reporting bugs and suggesting features</li>
            <li>• 📖 Contributing to the documentation</li>
            <li>• 💻 Submitting code contributions</li>
          </ul>
        </CardContent>
      </Card>
    </div>
  );
}