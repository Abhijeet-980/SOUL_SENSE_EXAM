'use client';

import * as React from 'react';
import { cn } from '@/lib/utils';

interface SwitchProps extends React.InputHTMLAttributes<HTMLInputElement> {
    onCheckedChange?: (checked: boolean) => void;
}

const Switch = React.forwardRef<HTMLInputElement, SwitchProps>(
    ({ className, onCheckedChange, ...props }, ref) => {
        const [checked, setChecked] = React.useState(props.defaultChecked || props.checked || false);

        const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
            const isChecked = e.target.checked;
            setChecked(isChecked);
            onCheckedChange?.(isChecked);
            props.onChange?.(e);
        };

        return (
            <label className="relative inline-flex items-center cursor-pointer">
                <input
                    type="checkbox"
                    className="sr-only peer"
                    ref={ref}
                    checked={props.checked !== undefined ? props.checked : checked}
                    onChange={handleChange}
                    {...props}
                />
                <div
                    className={cn(
                        "w-11 h-6 bg-muted peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-primary/20 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary",
                        className
                    )}
                ></div>
            </label>
        );
    }
);

Switch.displayName = 'Switch';

export { Switch };
