'use client';

import { BentoGridItem } from './bento-grid';
import { PlayCircle, PenLine, History, Target } from 'lucide-react';
import Link from 'next/link';
import { Button } from '@/components/ui/button';

export const QuickActions = () => {
    return (
        <BentoGridItem
            title="Quick Actions"
            description="Start a new session or check your progress."
            header={
                <div className="flex flex-col gap-2 p-2 h-full justify-center">
                    <Button asChild className="w-full justify-start gap-2" variant="outline">
                        <Link href="/exam">
                            <PlayCircle className="h-4 w-4" />
                            Start New Exam
                        </Link>
                    </Button>
                    <Button asChild className="w-full justify-start gap-2" variant="outline">
                        <Link href="/journal">
                            <PenLine className="h-4 w-4" />
                            Write Journal
                        </Link>
                    </Button>
                    <Button asChild className="w-full justify-start gap-2" variant="outline">
                        <Link href="/results">
                            <History className="h-4 w-4" />
                            View Results
                        </Link>
                    </Button>
                </div>
            }
            icon={<Target className="h-4 w-4" />}
            className="md:col-span-1"
        />
    );
};
