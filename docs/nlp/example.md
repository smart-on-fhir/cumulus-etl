---
title: Example Workflow
parent: NLP
grand_parent: ETL
nav_order: 1
# audience: engineer familiar with the project
# type: tutorial
---

# An Example NLP Workflow

Let's work through an end-to-end NLP workflow, as if you were doing a real study.
But we'll use an example study shipped with the ETL for testing purposes instead.

This will take us from the initial actual NLP, then to chart review,
then finally to analyzing accuracy.

You don't need to prepare your own clinical notes for this run-through.
We'll use synthetic notes shipped with Cumulus ETL for this very purpose.

This example study we'll use is just a very simple age range study.
The NLP will only be tasked with extracting an age from a clinical note.

## The NLP Itself

Before we start, you'll need to have Cumulus ETL and your AWS infrastructure ready.
Follow the [setup instructions](../setup) if you haven't done so already, then come back here.

### Model Setup

You have a choice of model for this.
Real studies might require one specific model or another.
But this example task is fairly liberal.

Here are the options, along with the task name to use.

#### Azure Cloud Options
For these, you'll want to set a couple variables first:
```sh
export AZURE_OPENAI_API_KEY=xxx
export AZURE_OPENAI_ENDPOINT=https://xxx.openai.azure.com/
```

Task names:
- GPT4: `example_nlp__nlp_gpt4`
- GPT4o: `example_nlp__nlp_gpt4o`
- GPT5: `example_nlp__nlp_gpt5`

This should cost you less than 15 cents to run and could be much less depending on the model.
We'll use less than five thousand tokens.

#### Local (On-Prem) Options
For these, you'll need to start up the appropriate model on your machine:
```sh
docker compose up --wait gpt-oss-120b
```

Task names:
- GPT-OSS 120B (needs 80GB of GPU memory): `example_nlp__nlp_gpt_oss_120b`

### Running the ETL

Now that your model is ready, let's run the ETL on some notes!

Below is the command line to use.
You'll need to change the bucket names and paths to wherever you set up your AWS infrastructure.
And you'll want to change the task name as appropriate for your model.
Leave the odd looking `%EXAMPLE-NLP%` bit in place;
that just tells Cumulus ETL to use its built-in example documents as the input.

The output and PHI bucket locations should be the same as your normal ETL runs on raw FHIR data.
There's no actual PHI in this example run because of the synthetic data,
but normally there is, and that PHI bucket is where Cumulus ETL keeps caches of NLP results.

```sh
docker compose run --rm \
  cumulus-etl nlp \
  %EXAMPLE-NLP% \
  s3://my-output-bucket/ \
  s3://my-phi-bucket/ \
  --task example_nlp__nlp_gpt4o
```

(If this were a real study, you'd probably do this a bit differently.
You'd point at your real DocumentReference resources for example.
And you'd probably restrict the set of documents you run NLP on with an argument
like `--select-by-athena-table study__my_cohort`.
But for this run-through, we're going to hand-wave all the document selection pieces.)

### Running the Crawler

Whenever you write a new table to S3, you'll want to run your AWS Glue crawler again,
so that the table's schema gets set correctly in Athena.

First, confirm that your AWS Cloud Formation templates have the `example_nlp__nlp_*` tables
configured in them. If not, try copying the Glue crawler definition from
[the sample template we provide](../setup/aws.md).

Then go to your AWS console, in the AWS Glue service, in the sidebar under Data Catalog, and
choose Crawlers.
You should see your crawler listed there. Select it, click Run, and wait for it to finish.

### Confirm the Data in Athena

While you're in the AWS console, switch to the Athena service and select the appropriate
Cumulus workgroup and database.

Then if you make a query like below (assuming you used the GPT4o model),
you should see eight results with extracted ages.
```sql
select * from example_nlp__nlp_gpt4o
```

**Congratulations!**
You've now run NLP on some synthetic clinical notes and uploaded the results to Athena.
Those extracted ages can now be post-processed by the `example` study to calculate age ranges,
and then confirmed with chart review by humans.

## Post-Processing of the NLP Results

Studies will normally need to post-process the results of the NLP,
which is usually performing some sort of knowledge extraction.
A study might combine bits of NLP-extracted knowledge together or
cross-reference with some structured data to create a study finding, or label.

In this example study we are running through,
we'll post-process the extracted ages into clinically-relevant age range labels.
Which we will afterward feed into Label Studio for human review.

So let's build the `example_nlp` study (shipped with Cumulus Library, for convenience):
```sh
cumulus-library build \
  --profile my-aws-profile \
  --database my_database \
  --workgroup my_workgroup \
  --target example_nlp
```

## Human Chart Review

Now let's validate the NLP results with a human review.
To do this, we'll use Label Studio.

### Label Studio Setup

THe Label Studio project itself has good
[install documentation](https://labelstud.io/guide/install.html).
We recommend you follow that and continue here once you have it installed.

You'll also need to prepare a Label Studio project to upload notes into.
Inside the main Label Studio dashboard, create a new project.
Give it a name and skip the data import step (we'll do that ourselves in a moment).

When it asks you to set up a labeling template,
click the "Custom Template" button on the bottom left.
Enter the following template, which lists the possible labels for our example study.
```
<View>
  <Labels name="label" toName="text">
    <Label value="infant (0-1)" background="#3584e4"/>
    <Label value="child (2-12)" background="#2190a4"/>
    <Label value="adolescent (13-18)" background="#3a944a"/>
    <Label value="young adult (19-24)" background="#c88800"/>
    <Label value="adult (25-44)" background="#ed5b00"/>
    <Label value="middle aged (45-64)" background="#e62d42"/>
    <Label value="aged (65+)" background="#d56199"/>
    <Label value="unknown" background="#6f8396"/>
  </Labels>
  <Text name="text" value="$text"/>
</View>
```

### Upload Your Notes

Now you'll want to fill in your new project with the annotated clinical notes.
Take a note of the project number (in the project's URL) and replace the number below,
along with the other arguments for your site.
```sh
export AWS_PROFILE=my-aws-profile
docker compose run --rm \
  cumulus-etl upload-notes \
  %EXAMPLE-NLP% \
  https://my-label-studio-host/ \
  s3://my-phi-bucket/ \
  --ls-project xx \
  --ls-token /path/to/token.txt \
  --athena-workgroup my_workgroup \
  --athena-database my_database \
  --label-by-athena-table example_nlp__range_labels
```

### Review the Charts

Refresh the project page in Label Studio and you should see a few notes in Label Studio.
Clicking on one should show a highlighted block of text with a labeled age range.

Add your own label to each of the notes.
You do this by clicking on the label at the top that you want to add,
then highlighting a region of text.
Then click the Submit button to finalize it.

You may agree or not with the NLP results.
In fact, even if the NLP is perfect here, why don't you intentionally disagree a few times.
It will make the number crunching below more interesting.

Once you have annotated all the notes, you have finished your chart review!

## Calculate Agreement

After any sort of chart review activity,
you'll want to measure the agreement between your reviewers/annotators.

We've built a tool called `chart-review` that can generate some nice statistics for this.

But first, let's gather up all our chart annotations.

### Export Annotations

Let's download those Label Studio annotations you made.
In Label Studio, click Export in the project and choose the default JSON format.
Save this file as `labelstudio-export.json`

Because the Label Studio export doesn't include all the NLP annotations,
you'll have to download those separately too, to be able to compare them to your annotations.
- Go to AWS Athena.
- Run a query like `select note_ref, label from example_nlp__range_labels`
- Download the result as `gpt4o.csv` (or whatever your model name was)
  - (If you ran multiple NLP models, you'll want to add a `where origin = '...'` to the
    above query and save each model's results in its own CSV file.)

OK, now you have everything you need.

### Create a Chart Review Config File

Chart Review doesn't need much configuration,
but you'll need to tell it where these exported NLP annotations are.

Create a `config.yaml` file alongside the exported annotations, with the following contents:
```yaml
annotators:
  gpt4o:
    filename: gpt4o.csv
```

### Running Chart Review

First, install the tool with `pipx install chart-review`.

Then, in the same folder as the other files, run `chart-review` without any arguments.
You should see ouput similar to:
```
~$ chart-review
╭───────────┬─────────────┬───────────╮
│ Annotator │ Chart Count │ Chart IDs │
├───────────┼─────────────┼───────────┤
│ 1         │ 8           │ 1305–1312 │
│ gpt4o     │ 8           │ 1305–1312 │
╰───────────┴─────────────┴───────────╯
```

"Annotator 1" (or whatever number you see) is you, as your Label Studio ID.
You can give yourself a convenient name by editing `config.yaml` again to look like:
```yaml
annotators:
  me: 1
  gpt4o:
    filename: gpt4o.csv
```

Let's run chart-review again:
```
~$ chart-review
╭───────────┬─────────────┬───────────╮
│ Annotator │ Chart Count │ Chart IDs │
├───────────┼─────────────┼───────────┤
│ gpt4o     │ 8           │ 1305–1312 │
│ me        │ 8           │ 1305–1312 │
╰───────────┴─────────────┴───────────╯
```

OK so that's not so impressive. That's some very basic information indeed.
Let's get some agreement information
(in this example, I disagreed with the model a fair bit, for demonstration purposes):
```
~$ chart-review accuracy me gpt4o
Comparing 8 charts (1305–1312)
Truth: me
Annotator: gpt4o
Macro F1: 0.25

F1     Sens  Spec   PPV   NPV    Kappa  TP  FN  TN  FP  Label              
0.333  0.5   0.786  0.25  0.917  0.2    2   2   22  6   *                  
0      0     0      0     0      0      0   1   7   0   adolescent (13-18) 
0.667  1.0   0.857  0.5   1.0    0.6    1   0   6   1   child (2-12)       
0.333  1.0   0.429  0.2   1.0    0.158  1   0   3   4   middle aged (45-64)
0      0     0      0     0      0      0   1   6   1   young adult (19-24)
```

Now we can see some F1 scores and all sorts of interesting stats.
Chart Review has other fun features.
You can explore its command line options with `--help`.

And that's it! You've gone from some source documents, through NLP, through human chart review,
and finally you calculated how well NLP agreed with you, the human.

In a real study, obviously all these steps would be more interesting than this toy example.
But the flow of data through the Cumulus pipeline would have the same shape.
