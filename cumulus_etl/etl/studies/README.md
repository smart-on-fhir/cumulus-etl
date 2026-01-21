These study folders define study-specific NLP tasks to run, using a TOML config file.

Config syntax:

- `task` (array): parent table to a single task, usually specified like `[[task]]`
- `task.name` (str): name of task, used in the Athena table name, so it needs to be simple.
  Can be omitted.
- `task.version` (int): task version, used in the cache key
- `task.system-prompt` (str): system prompt to send to the model,
   with some allowed keyword substitutions:
   - `%JSON-SCHEMA%`: this will expand to your response schema
- `task.user-prompt` (str): user prompt to send to the model,
   with some allowed keyword substitions:
   - `%CLINICAL-NOTE%`: this will expand to the note under consideration
   If omitted, just the clinical note will be used. (i.e. the default is `"%CLINICAL-NOTE%"`)
- `task.models` (list of str): NLP model names to support for this task
- `task.response-schema` (str): a path to JSON schema used to validate the NLP response
- `shared.system-prompt` (str): a default system prompt to use for any task that doesn't define one
- `shared.user-prompt` (str): a default user prompt to use for any task that doesn't define one
- `shared.models` (list of str): a default model list to use for any task that doesn't define one

All fields in the response-schema named `spans` will be assumed to be a list of text spans from the
note and converted to a list of integer-offset spans.

When the source study updates its pydantic models,
it should regenerate their JSON schemas and drop them here.
