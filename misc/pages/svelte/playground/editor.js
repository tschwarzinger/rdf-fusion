import { EditorView, basicSetup } from 'codemirror';
import { sparql } from 'codemirror-lang-sparql';

// Shared CodeMirror editor factory for the query editor and the read-only preview.
export function createEditor(container, { doc = '', readonly = false } = {}) {
    const extensions = [basicSetup, sparql()];
    if (readonly) {
        extensions.push(EditorView.editable.of(false));
    }
    return new EditorView({
        doc,
        extensions,
        parent: container
    });
}
