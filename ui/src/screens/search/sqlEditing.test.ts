import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { CompletionSelections } from './sqlEditing.ts';

test('completion undo restores the original caret without replacing browser text history', () => {
  const history = new CompletionSelections();
  history.record('se', 'SELECT ', [2, 2], [7, 7]);
  assert.deepEqual(history.input('historyUndo', 'SELECT ', 'se'), [2, 2]);
  assert.deepEqual(history.input('historyRedo', 'se', 'SELECT '), [7, 7]);
  assert.equal(history.input('insertText', 'SELECT ', 'SELECT x'), null);
  assert.equal(history.input('historyUndo', 'SELECT x', 'SELECT '), null);
  assert.deepEqual(history.input('historyUndo', 'SELECT ', 'se'), [2, 2]);
});

test('branch edits and external replacement discard stale completion selections', () => {
  const history = new CompletionSelections();
  history.record('se', 'SELECT ', [2, 2], [7, 7]);
  history.input('historyUndo', 'SELECT ', 'se');
  history.input('insertFromPaste', 'se', 'select');
  assert.equal(history.input('historyRedo', 'se', 'SELECT '), null);
  history.record('fr', 'FROM ', [1, 1], [5, 5]);
  history.clear();
  assert.equal(history.input('historyUndo', 'FROM ', 'fr'), null);
});

test('repeated completions restore their own carets and do not intercept unrelated undo', () => {
  const history = new CompletionSelections();
  history.record('se', 'SELECT ', [2, 2], [7, 7]);
  history.record('SELECT fr', 'SELECT FROM ', [9, 9], [12, 12]);
  assert.deepEqual(history.input('historyUndo', 'SELECT FROM ', 'SELECT fr'), [9, 9]);
  assert.equal(history.input('historyUndo', 'SELECT fr', 'SELECT '), null);
  assert.deepEqual(history.input('historyUndo', 'SELECT ', 'se'), [2, 2]);
  assert.equal(history.input('historyUndo', 'se', ''), null);
});
