import pandas as pd
import re
import os
from textblob import TextBlob
from deep_translator import GoogleTranslator
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)


def clean_lyrics(text):

    """
    Cleans song lyrics by removing metadata and formatting.

    Parameters
    ----------
    text : str
        Raw lyrics text.

    Returns
    -------
    str or None
        Cleaned lyrics or None if input is not a string.
    """
    if not isinstance(text, str):
        return None
    text = re.sub(r"\[.*?\]|\(.*?\)", "", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def batch_translate(texts):

    """
    Efficient batch translation of lyrics.

    Parameters
    ----------
    texts : list
        List of texts to translate.

    Returns
    -------
    list
        Translated texts or None for failed translations.
    """
    try:
        translator = GoogleTranslator(source='auto', target='en')
        return [translator.translate(text) if pd.notnull(text) 
                else None for text in texts]
    except Exception as e:
        logger.warning(f"Translation error: {str(e)}")
        return [None] * len(texts)


def analyze_sentiment(text):

    """
    Enhanced sentiment analysis with polarity, subjectivity and classification.

    Parameters
    ----------
    text : str
        Text for sentiment analysis.

    Returns
    -------
    dict or None
        Sentiment metrics or None if analysis fails.
    """
    try:
        blob = TextBlob(text)
        return {
            'polarity': blob.sentiment.polarity,
            'subjectivity': blob.sentiment.subjectivity,
            'sentiment': 'positive' if blob.sentiment.polarity > 0 else 
                        'negative' if blob.sentiment.polarity < 0 else 'neutral'
        }
    except Exception as e:
        logger.warning(f"Sentiment analysis failed for text: {str(e)}")
        return None


def run_sentiment_pipeline(input_path, output_path, lyrics_col="genius_lyrics", translate=True):

    """
    Complete sentiment analysis pipeline with validation and logging.

    Parameters
    ----------
    input_path : str
        Path to input CSV file.
    output_path : str
        Path to save output CSV file.
    lyrics_col : str
        Name of the column containing lyrics (default: 'genius_lyrics').
    translate : bool
        Whether to translate lyrics to English (default: True).

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with sentiment analysis results.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    sample = pd.read_csv(input_path, nrows=1)
    if lyrics_col not in sample.columns:
        raise ValueError(f"Column '{lyrics_col}' not found in input file")

    logger.info("Loading and cleaning data...")
    df = pd.read_csv(input_path)
    tqdm.pandas()
    df["cleaned_lyrics"] = df[lyrics_col].progress_apply(clean_lyrics)

    if translate:
        logger.info("Translating lyrics to English...")
        df["translated_lyrics"] = batch_translate(df["cleaned_lyrics"].tolist())
        sentiment_input = df["translated_lyrics"].combine_first(df["cleaned_lyrics"])
    else:
        sentiment_input = df["cleaned_lyrics"]

    logger.info("Analyzing sentiment...")
    sentiment_results = sentiment_input.progress_apply(analyze_sentiment)
    df = pd.concat([df, sentiment_results.apply(pd.Series)], axis=1)

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    df.to_csv(output_path, index=False)
    logger.info(f"Successfully saved results to {output_path}")
    return df


def run_sentiment_task(**kwargs):

    """
    Airflow task to run sentiment analysis pipeline.

    Parameters
    ----------
    kwargs : dict
        Airflow context including TaskInstance (ti).

    Returns
    -------
    None
    """
    ti = kwargs["ti"]
    enriched_path = ti.xcom_pull(task_ids="extract_api_merge_task", key="enriched_path")
    
    if not enriched_path:
        raise ValueError("No enriched path received from Genius task")

    output_path = "data/eda/spotify_genius_sentiment_eda.csv"

    run_sentiment_pipeline(
        input_path=enriched_path,
        output_path=output_path,
        lyrics_col="genius_lyrics",
        translate=True
    )

    ti.xcom_push(key="sentiment_output", value=output_path)
