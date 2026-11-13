type Selection = readonly [number, number];
type Completion = { before: string; after: string; start: Selection; end: Selection; undone: boolean };

/** Only restores completion selections. The browser owns all text undo/redo. */
export class CompletionSelections {
  private entries: Completion[] = [];

  record(before: string, after: string, start: Selection, end: Selection): void {
    this.entries = this.entries.filter(e => !e.undone);
    this.entries.push({ before, after, start, end, undone: false });
  }

  input(kind: string, before: string, after: string): Selection | null {
    const undo = kind === 'historyUndo';
    if (!undo && kind !== 'historyRedo') {
      // An edit after undo forks the native history, including paste and composition.
      this.entries = this.entries.filter(e => !e.undone);
      return null;
    }
    const candidates = undo ? [...this.entries].reverse() : this.entries;
    const entry = candidates.find(e => e.undone !== undo
      && before === (undo ? e.after : e.before) && after === (undo ? e.before : e.after));
    if (!entry) return null;
    entry.undone = undo;
    return undo ? entry.start : entry.end;
  }

  clear(): void { this.entries = []; }
}

/** A native editing command keeps the browser history; setRangeText/value do not. */
export function insertCompletion(input: HTMLTextAreaElement, start: number, end: number, text: string): boolean {
  const original: Selection = [input.selectionStart, input.selectionEnd];
  input.setSelectionRange(start, end);
  const inserted = input.ownerDocument.execCommand('insertText', false, text);
  if (!inserted) input.setSelectionRange(...original);
  return inserted;
}
