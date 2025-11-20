# =============================================================================
# HS to NAICS Concordance using concordance package
# Called from Stata for replicability
# =============================================================================

# Usage: Rscript hs_to_naics_concordance.R [input_path] [output_path]

library(concordance)
library(tidyverse)

# Get command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 2) {
  # Default paths if not provided
  input_path <- "C:/Users/dafrb/Desktop/EAM_data/china-schock/raw_data/concordance"
  output_path <- "C:/Users/dafrb/Desktop/EAM_data/china-schock/raw_data/concordance"
} else {
  input_path <- args[1]
  output_path <- args[2]
}

cat("=== HS to NAICS Concordance ===\n")
cat("Input path:", input_path, "\n")
cat("Output path:", output_path, "\n")

# =============================================================================
# Get unique HS6 codes from trade data
# =============================================================================

# Read the HS6 codes from the existing concordance (or could read from CSV)
# For now, we'll extract from the existing temp file
hs6_naics_temp <- haven::read_dta(file.path(input_path, "hs6_naics_temp.dta"))
hs6_codes <- unique(hs6_naics_temp$hs6)

cat("\nProcessing", length(hs6_codes), "unique HS6 codes\n")

# =============================================================================
# Convert HS to NAICS using concordance package
# =============================================================================

# The package supports multiple HS revisions
# We'll try HS5 (2017) as primary since most recent data uses this
# For older codes, we try progressively older revisions

convert_hs_to_naics <- function(hs_code) {
  hs_str <- sprintf("%06d", hs_code)

  # Try HS revisions from newest to oldest
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

# Process all HS6 codes
cat("\nConverting HS to NAICS (this may take a few minutes)...\n")

results <- data.frame(
  hs6 = integer(),
  naics2017 = character(),
  hs_revision_used = character(),
  stringsAsFactors = FALSE
)

# Process with progress updates
n_codes <- length(hs6_codes)
progress_interval <- ceiling(n_codes / 10)

for (i in seq_along(hs6_codes)) {
  hs <- hs6_codes[i]
  conversion <- convert_hs_to_naics(hs)

  results <- rbind(results, data.frame(
    hs6 = hs,
    naics2017 = conversion$naics,
    hs_revision_used = conversion$hs_revision,
    stringsAsFactors = FALSE
  ))

  if (i %% progress_interval == 0) {
    cat(sprintf("  Progress: %d/%d (%.0f%%)\n", i, n_codes, i/n_codes*100))
  }
}

# =============================================================================
# Summary and save
# =============================================================================

cat("\n=== Conversion Summary ===\n")
cat("Total HS6 codes:", nrow(results), "\n")
cat("Successfully mapped:", sum(!is.na(results$naics2017)), "\n")
cat("Failed to map:", sum(is.na(results$naics2017)), "\n")

cat("\nHS revision usage:\n")
print(table(results$hs_revision_used, useNA = "ifany"))

# Save results
output_file <- file.path(output_path, "hs6_naics2017_concordance.csv")
write.csv(results, output_file, row.names = FALSE)
cat("\nSaved to:", output_file, "\n")

# Also save as Stata format for direct use
output_dta <- file.path(output_path, "hs6_naics2017_concordance.dta")
haven::write_dta(results, output_dta)
cat("Saved to:", output_dta, "\n")

cat("\n=== Done ===\n")
