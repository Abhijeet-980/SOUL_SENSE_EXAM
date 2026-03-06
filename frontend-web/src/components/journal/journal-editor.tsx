"use client";

import React, { useState, useEffect, ChangeEvent } from "react";

interface JournalEditorProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  minHeight?: number;
  maxLength?: number;
}

const JournalEditor: React.FC<JournalEditorProps> = ({
  value,
  onChange,
  placeholder = "Write your thoughts...",
  minHeight = 120,
  maxLength,
}) => {
  const [text, setText] = useState<string>(value);
  const [isSaving, setIsSaving] = useState<boolean>(false);

  const handleChange = (e: ChangeEvent<HTMLTextAreaElement>) => {
    let newValue = e.target.value;

    if (maxLength) {
      newValue = newValue.slice(0, maxLength);
    }

    setText(newValue);
    onChange(newValue);

    setIsSaving(true);
    setTimeout(() => setIsSaving(false), 800);
  };

  useEffect(() => {
    setText(value);
  }, [value]);

  return (
    <div style={{ width: "100%" }}>
      <textarea
        value={text}
        onChange={handleChange}
        placeholder={placeholder}
        style={{
          width: "100%",
          minHeight: minHeight,
          resize: "vertical",
          padding: "10px",
          fontSize: "14px",
        }}
      />

      {maxLength && (
        <div style={{ fontSize: "12px", marginTop: "4px" }}>
          {text.length}/{maxLength}
        </div>
      )}

      <div style={{ fontSize: "12px", color: "gray" }}>
        {isSaving ? "Saving..." : "Saved"}
      </div>
    </div>
  );
};

export default JournalEditor;