.headers on
.mode csv
.output transcript_quality_sample.csv

-- 5 shortest
SELECT
  transcript_id,
  video_id,
  word_count,
  length(full_text) AS chars,
  language,
  transcript_provider,
  transcript_model,
  transcript_version,
  transcribed_at,
  full_text
FROM transcripts
WHERE full_text IS NOT NULL AND trim(full_text) != ''
ORDER BY COALESCE(word_count, length(full_text)) ASC
LIMIT 5;

-- 5 longest
SELECT
  transcript_id,
  video_id,
  word_count,
  length(full_text) AS chars,
  language,
  transcript_provider,
  transcript_model,
  transcript_version,
  transcribed_at,
  full_text
FROM transcripts
WHERE full_text IS NOT NULL AND trim(full_text) != ''
ORDER BY COALESCE(word_count, length(full_text)) DESC
LIMIT 5;

-- 5 middle-length
WITH ranked AS (
  SELECT
    transcript_id,
    video_id,
    word_count,
    length(full_text) AS chars,
    language,
    transcript_provider,
    transcript_model,
    transcript_version,
    transcribed_at,
    full_te    full_te    full_te    full_te    full_te    f_count, length    fullxt    full_te    full_(*) OVER () AS cnt
    full_te cripts
                  I           AND trim(full_t                  I      nscript_id,
  vid  vid  vid  vid  vid     vid  vid  vid  ,
  transcript_provider,
  transcript_model  transcriri  transcon,
  transcribed_at,
  full_text
FROM ranked
WHERE rn BETWEEN (cnt/2 - 2) AND (cnt/2 + 2);

-- 5 random
SELECT
  transcript_id,
  video_id,
  word_count,
  length(full_text)   length(fulla  length(full_text)   lengthr,  length(fip  length(full_textipt_v  length(fulanscribe  length(full_tet
FROMFROMFROMFpts
WHEREWHEREWHExt IS NOT NULL AND trim(full_text) != ''
ORDER BY random()
LIMIT 5;

.output stdout
