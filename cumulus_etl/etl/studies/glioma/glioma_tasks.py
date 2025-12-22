"""Define tasks for the irae study"""

from cumulus_etl import nlp
from cumulus_etl.etl import tasks
from cumulus_etl.etl.studies.glioma.glioma_models import (
    GliomaCaseAnnotation,
)

###############################################################################
# Base IRAE Tasks
# These base classes define common behavior and prompts for the IRAE study tasks.
###############################################################################


class BaseGliomaTask(tasks.BaseModelTaskWithSpans):
    # Task Version History:
    # ** 0 (2025-12): Initial version **
    task_version = 0

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

    response_format = GliomaCaseAnnotation


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
###############################################################################


class GliomaGpt4oTask(BaseGliomaTask):
    name = "glioma__nlp_gpt4o"
    client_class = nlp.Gpt4oModel


class GliomaGpt5Task(BaseGliomaTask):
    name = "glioma__nlp_gpt5"
    client_class = nlp.Gpt5Model


class GliomaGptOss120bTask(BaseGliomaTask):
    name = "glioma__nlp_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class GliomaLlama4ScoutTask(BaseGliomaTask):
    name = "glioma__nlp_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class GliomaClaudeSonnet45Task(BaseGliomaTask):
    name = "glioma__nlp_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model
