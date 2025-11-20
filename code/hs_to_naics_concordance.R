# HS to NAICS Concordance using concordance package
# Called from Stata

library(concordance)
library(tidyverse)

args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 2) {

  input_path <- "C:/Users/dafrb/Desktop/EAM_data/china-schock/raw_data/concordance"
  output_path <- "C:/Users/dafrb/Desktop/EAM_data/china-schock/raw_data/concordance"
} else {
  input_path <- args[1]
  output_path <- args[2]
}

# Load HS6 codes from existing temp file
hs6_naics_temp <- haven::read_dta(file.path(input_path, "hs6_naics_temp.dta"))
hs6_codes <- unique(hs6_naics_temp$hs6)

# Convert HS to NAICS (try multiple HS revisions)
convert_hs_to_naics <- function(hs_code) {
  hs_str <- sprintf("%06d", hs_code)

  for (hs_rev in c("HS5", "HS4", "HS3", "HS2", "HS1", "HS0")) {
    result <- tryCatch({
      r <- concord(hs_str, hs_rev, "NAICS2017", dest.digit = 6)
      if (length(r) > 0 && !all(is.na(r))) {
        return(list(naics = r[1], hs_revision = hs_rev))
      }
      NULL
    }, error = function(e) NULL)

    if (!is.null(result)) return(result)
  }

  return(list(naics = NA_character_, hs_revision = NA_character_))
}

# Process all codes
results <- map_dfr(hs6_codes, function(hs) {
  conversion <- convert_hs_to_naics(hs)
  data.frame(
    hs6 = hs,
    naics2017 = conversion$naics,
    hs_revision_used = conversion$hs_revision,
    stringsAsFactors = FALSE
  )
})

# Save results
write.csv(results, file.path(output_path, "hs6_naics2017_concordance.csv"), row.names = FALSE)
haven::write_dta(results, file.path(output_path, "hs6_naics2017_concordance.dta"))
