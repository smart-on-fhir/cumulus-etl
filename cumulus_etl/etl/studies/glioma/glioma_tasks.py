"""Define tasks for the irae study"""

from cumulus_etl import nlp
from cumulus_etl.etl import tasks
from cumulus_etl.etl.studies.glioma.glioma_diagnosis_models import (
    GliomaDiagnosisAnnotation,
)
from cumulus_etl.etl.studies.glioma.glioma_doctype_models import (
    GliomaDocumentTypeAnnotation,
)
from cumulus_etl.etl.studies.glioma.glioma_gene_models import (
    GliomaGeneAnnotation,
)
from cumulus_etl.etl.studies.glioma.glioma_med_models import (
    GliomaMedicationsAnnotation,
)
from cumulus_etl.etl.studies.glioma.glioma_progression_models import (
    GliomaProgressionAnnotation,
)
from cumulus_etl.etl.studies.glioma.glioma_surgery_models import (
    GliomaSurgicalAnnotation,
)

###############################################################################
# Base Glioma Tasks
# These base classes define common behavior and prompts for the Glioma study tasks.
###############################################################################


class BaseGliomaTask(tasks.BaseModelTaskWithSpans):
    # Task Version History:
    # **   (2026-01): Task split, versions moved to subclasses
    # ** 0 (2025-12): Initial version **
    system_prompt = (
        "You are a clinical chart reviewer for a study examining the efficacy of various "
        "treatments for pediatric low-grade glioma (pLGG) across various pathological/genetic "
        "subtypes.\n"
        "Your task is to extract patient-specific information from an unstructured clinical "
        "document and map it into a predefined Pydantic schema.\n"
        "\n"
        "Core Rules:\n"
        "1. Base all assertions ONLY on patient-specific information in the clinical document.\n"
        "   - Never negate or exclude information just because it is not mentioned.\n"
        "   - Never conflate family history or population-level risk with patient findings.\n"
        "   - Do not count past medical history, prior episodes, or family history.\n"
        "2. Do not invent or infer facts beyond what is documented.\n"
        "3. Maintain high fidelity to the clinical document language when citing spans.\n"
        "4. Answer patient outcomes with strongest available documented evidence:\n"
        "5. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
        "\n"
        "Pydantic Schema:\n"
        "%JSON-SCHEMA%"
    )
    user_prompt = (
        "Evaluate the following clinical document for glioma variables and outcomes.\n"
        "Here is the clinical document for you to analyze:\n"
        "\n"
        "%CLINICAL-NOTE%"
    )


###############################################################################
# Model-Specific Tasks
#
# For each base Glioma task, we define specific tasks for each NLP model.
# Models supported include:
#   - Gpt4o
#   - Gpt5
#   - GptOss120b
#   - Llama4Scout
#   - ClaudeSonnet45
#
# There are 7 glioma tasks:
#   - Document Type Classification
#   - Diagnosis Related Information Extraction
#   - Genetic Marker and Molecular Driver Information Extraction
#   - Glioma Drugs - Chemotherapy and Targeted Therapy Information Extraction
#   - Progression Related Information Extraction
#   - Surgery Related Information Extraction
###############################################################################


class BaseGliomaDocumentTypeTask(BaseGliomaTask):
    # Task Version History:
    # **   (2026-01): Task split, versions moved to subclasses
    task_version = 1
    response_format = GliomaDocumentTypeAnnotation


class BaseGliomaDiagnosisTask(BaseGliomaTask):
    # Task Version History:
    # **   (2026-01): Task split, versions moved to subclasses
    task_version = 1
    response_format = GliomaDiagnosisAnnotation


class BaseGliomaGeneTask(BaseGliomaTask):
    # Task Version History:
    # **   (2026-01): Task split, versions moved to subclasses
    task_version = 1
    response_format = GliomaGeneAnnotation


class BaseGliomaMedicationsTask(BaseGliomaTask):
    # Task Version History:
    # **   (2026-01): Task split, versions moved to subclasses
    task_version = 1
    response_format = GliomaMedicationsAnnotation


class BaseGliomaProgressionTask(BaseGliomaTask):
    # Task Version History:
    # **   (2026-01): Task split, versions moved to subclasses
    task_version = 1
    response_format = GliomaProgressionAnnotation


class BaseGliomaSurgicalTask(BaseGliomaTask):
    # Task Version History:
    # **   (2026-01): Task split, versions moved to subclasses
    task_version = 1
    response_format = GliomaSurgicalAnnotation


###############################################################################
# GliomaDocumentTypeTasks
class GliomaDocumentTypeTaskGpt4oTask(BaseGliomaDocumentTypeTask):
    name = "glioma__nlp_document_type_gpt4o"
    client_class = nlp.Gpt4oModel


class GliomaDocumentTypeTaskGpt5Task(BaseGliomaDocumentTypeTask):
    name = "glioma__nlp_document_type_gpt5"
    client_class = nlp.Gpt5Model


class GliomaDocumentTypeTaskGptOss120bTask(BaseGliomaDocumentTypeTask):
    name = "glioma__nlp_document_type_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class GliomaDocumentTypeTaskLlama4ScoutTask(BaseGliomaDocumentTypeTask):
    name = "glioma__nlp_document_type_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class GliomaDocumentTypeTaskClaudeSonnet45Task(BaseGliomaDocumentTypeTask):
    name = "glioma__nlp_document_type_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


###############################################################################
# GliomaDiagnosisTasks
class GliomaDiagnosisTaskGpt4oTask(BaseGliomaDiagnosisTask):
    name = "glioma__nlp_diagnosis_gpt4o"
    client_class = nlp.Gpt4oModel


class GliomaDiagnosisTaskGpt5Task(BaseGliomaDiagnosisTask):
    name = "glioma__nlp_diagnosis_gpt5"
    client_class = nlp.Gpt5Model


class GliomaDiagnosisTaskGptOss120bTask(BaseGliomaDiagnosisTask):
    name = "glioma__nlp_diagnosis_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class GliomaDiagnosisTaskLlama4ScoutTask(BaseGliomaDiagnosisTask):
    name = "glioma__nlp_diagnosis_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class GliomaDiagnosisTaskClaudeSonnet45Task(BaseGliomaDiagnosisTask):
    name = "glioma__nlp_diagnosis_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


###############################################################################
# GliomaGeneTasks
class GliomaGeneTaskGpt4oTask(BaseGliomaGeneTask):
    name = "glioma__nlp_gene_gpt4o"
    client_class = nlp.Gpt4oModel


class GliomaGeneTaskGpt5Task(BaseGliomaGeneTask):
    name = "glioma__nlp_gene_gpt5"
    client_class = nlp.Gpt5Model


class GliomaGeneTaskGptOss120bTask(BaseGliomaGeneTask):
    name = "glioma__nlp_gene_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class GliomaGeneTaskLlama4ScoutTask(BaseGliomaGeneTask):
    name = "glioma__nlp_gene_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class GliomaGeneTaskClaudeSonnet45Task(BaseGliomaGeneTask):
    name = "glioma__nlp_gene_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


###############################################################################
# GliomaMedicationsTasks
class GliomaMedicationsTaskGpt4oTask(BaseGliomaMedicationsTask):
    name = "glioma__nlp_medications_gpt4o"
    client_class = nlp.Gpt4oModel


class GliomaMedicationsTaskGpt5Task(BaseGliomaMedicationsTask):
    name = "glioma__nlp_medications_gpt5"
    client_class = nlp.Gpt5Model


class GliomaMedicationsTaskGptOss120bTask(BaseGliomaMedicationsTask):
    name = "glioma__nlp_medications_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class GliomaMedicationsTaskLlama4ScoutTask(BaseGliomaMedicationsTask):
    name = "glioma__nlp_medications_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class GliomaMedicationsTaskClaudeSonnet45Task(BaseGliomaMedicationsTask):
    name = "glioma__nlp_medications_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


###############################################################################
# GliomaProgressionTasks
class GliomaProgressionTaskGpt4oTask(BaseGliomaProgressionTask):
    name = "glioma__nlp_progression_gpt4o"
    client_class = nlp.Gpt4oModel


class GliomaProgressionTaskGpt5Task(BaseGliomaProgressionTask):
    name = "glioma__nlp_progression_gpt5"
    client_class = nlp.Gpt5Model


class GliomaProgressionTaskGptOss120bTask(BaseGliomaProgressionTask):
    name = "glioma__nlp_progression_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class GliomaProgressionTaskLlama4ScoutTask(BaseGliomaProgressionTask):
    name = "glioma__nlp_progression_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class GliomaProgressionTaskClaudeSonnet45Task(BaseGliomaProgressionTask):
    name = "glioma__nlp_progression_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


###############################################################################
# GliomaSurgicalTasks
class GliomaSurgicalTaskGpt4oTask(BaseGliomaSurgicalTask):
    name = "glioma__nlp_surgical_gpt4o"
    client_class = nlp.Gpt4oModel


class GliomaSurgicalTaskGpt5Task(BaseGliomaSurgicalTask):
    name = "glioma__nlp_surgical_gpt5"
    client_class = nlp.Gpt5Model


class GliomaSurgicalTaskGptOss120bTask(BaseGliomaSurgicalTask):
    name = "glioma__nlp_surgical_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class GliomaSurgicalTaskLlama4ScoutTask(BaseGliomaSurgicalTask):
    name = "glioma__nlp_surgical_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class GliomaSurgicalTaskClaudeSonnet45Task(BaseGliomaSurgicalTask):
    name = "glioma__nlp_surgical_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model
