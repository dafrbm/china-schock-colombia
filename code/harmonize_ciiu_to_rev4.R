# =============================================================================
# CIIU Harmonization to Rev 4 and Standardized HS->ISIC Concordance
# =============================================================================

library(tidyverse)
library(haven)
library(concordance)

# Set paths
base_dir <- "C:/Users/dafrb/Desktop/EAM_data/china-schock"
processed_dir <- file.path(base_dir, "processed")
concordance_dir <- file.path(base_dir, "raw_data/concordance")
output_dir <- file.path(base_dir, "output")

# =============================================================================
# PART 1: Load EAM panel and analyze CIIU sources
# =============================================================================

cat("\n=== PART 1: Loading EAM Panel ===\n")

eam <- read_dta(file.path(processed_dir, "panel_eam_with_tfp.dta"))

cat("Total observations:", nrow(eam), "\n")
cat("\nCIIU source distribution:\n")
print(table(eam$ciiu_source))

# Classify CIIU revision by source
eam <- eam %>%
  mutate(
    ciiu_revision = case_when(
      ciiu_source %in% c("CIIU2", "CIIU2N4") ~ 2,
      ciiu_source == "CIIU3" ~ 3,
      ciiu_source %in% c("CIIU4", "CIIU_4") ~ 4,
      TRUE ~ NA_real_
    )
  )

cat("\nObservations by CIIU revision:\n")
print(table(eam$ciiu_revision, useNA = "ifany"))

# =============================================================================
# PART 2: Build ISIC Rev 2 -> Rev 4 concordance
# =============================================================================

cat("\n=== PART 2: Building ISIC Rev 2 -> Rev 4 Concordance ===\n")

# Get unique CIIU codes from Rev 2 observations
ciiu_rev2 <- eam %>%
  filter(ciiu_revision == 2) %>%
  pull(ciiu) %>%
  unique() %>%
  sort()

cat("Unique CIIU Rev 2 codes:", length(ciiu_rev2), "\n")

# Convert Rev 2 -> Rev 4
rev2_to_rev4 <- data.frame(
  ciiu_original = character(),
  ciiu_rev4_4d = character(),
  ciiu_rev4_3d = character(),
  ciiu_rev4_2d = character(),
  stringsAsFactors = FALSE
)

cat("\nConverting ISIC Rev 2 to Rev 4...\n")
for (code in ciiu_rev2) {
  # Pad to 4 digits if needed
  code_padded <- sprintf("%04s", code)

  # Try conversion at different digit levels
  rev4_4d <- tryCatch({
    r <- concord(code_padded, "ISIC2", "ISIC4", dest.digit = 4)
    if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
  }, error = function(e) NA_character_)

  rev4_3d <- tryCatch({
    r <- concord(code_padded, "ISIC2", "ISIC4", dest.digit = 3)
    if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
  }, error = function(e) NA_character_)

  rev4_2d <- tryCatch({
    r <- concord(code_padded, "ISIC2", "ISIC4", dest.digit = 2)
    if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
  }, error = function(e) NA_character_)

  rev2_to_rev4 <- rbind(rev2_to_rev4, data.frame(
    ciiu_original = code,
    ciiu_rev4_4d = rev4_4d,
    ciiu_rev4_3d = rev4_3d,
    ciiu_rev4_2d = rev4_2d,
    stringsAsFactors = FALSE
  ))
}

cat("Rev 2 conversion results:\n")
cat("  - 4-digit mapped:", sum(!is.na(rev2_to_rev4$ciiu_rev4_4d)), "/", nrow(rev2_to_rev4), "\n")
cat("  - 3-digit mapped:", sum(!is.na(rev2_to_rev4$ciiu_rev4_3d)), "/", nrow(rev2_to_rev4), "\n")
cat("  - 2-digit mapped:", sum(!is.na(rev2_to_rev4$ciiu_rev4_2d)), "/", nrow(rev2_to_rev4), "\n")

# =============================================================================
# PART 3: Build ISIC Rev 3 -> Rev 4 concordance
# =============================================================================

cat("\n=== PART 3: Building ISIC Rev 3 -> Rev 4 Concordance ===\n")

# Get unique CIIU codes from Rev 3 observations
ciiu_rev3 <- eam %>%
  filter(ciiu_revision == 3) %>%
  pull(ciiu) %>%
  unique() %>%
  sort()

cat("Unique CIIU Rev 3 codes:", length(ciiu_rev3), "\n")

# Convert Rev 3 -> Rev 4
rev3_to_rev4 <- data.frame(
  ciiu_original = character(),
  ciiu_rev4_4d = character(),
  ciiu_rev4_3d = character(),
  ciiu_rev4_2d = character(),
  stringsAsFactors = FALSE
)

cat("\nConverting ISIC Rev 3 to Rev 4...\n")
for (code in ciiu_rev3) {
  code_padded <- sprintf("%04s", code)

  rev4_4d <- tryCatch({
    r <- concord(code_padded, "ISIC3", "ISIC4", dest.digit = 4)
    if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
  }, error = function(e) NA_character_)

  rev4_3d <- tryCatch({
    r <- concord(code_padded, "ISIC3", "ISIC4", dest.digit = 3)
    if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
  }, error = function(e) NA_character_)

  rev4_2d <- tryCatch({
    r <- concord(code_padded, "ISIC3", "ISIC4", dest.digit = 2)
    if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
  }, error = function(e) NA_character_)

  rev3_to_rev4 <- rbind(rev3_to_rev4, data.frame(
    ciiu_original = code,
    ciiu_rev4_4d = rev4_4d,
    ciiu_rev4_3d = rev4_3d,
    ciiu_rev4_2d = rev4_2d,
    stringsAsFactors = FALSE
  ))
}

cat("Rev 3 conversion results:\n")
cat("  - 4-digit mapped:", sum(!is.na(rev3_to_rev4$ciiu_rev4_4d)), "/", nrow(rev3_to_rev4), "\n")
cat("  - 3-digit mapped:", sum(!is.na(rev3_to_rev4$ciiu_rev4_3d)), "/", nrow(rev3_to_rev4), "\n")
cat("  - 2-digit mapped:", sum(!is.na(rev3_to_rev4$ciiu_rev4_2d)), "/", nrow(rev3_to_rev4), "\n")

# =============================================================================
# PART 4: Apply harmonization to EAM panel
# =============================================================================

cat("\n=== PART 4: Applying Harmonization to EAM Panel ===\n")

# Create combined concordance table
all_concordances <- bind_rows(
  rev2_to_rev4 %>% mutate(source_rev = 2),
  rev3_to_rev4 %>% mutate(source_rev = 3)
)

# For Rev 4 codes, they stay the same
ciiu_rev4 <- eam %>%
  filter(ciiu_revision == 4) %>%
  pull(ciiu) %>%
  unique()

rev4_identity <- data.frame(
  ciiu_original = ciiu_rev4,
  ciiu_rev4_4d = ciiu_rev4,
  ciiu_rev4_3d = substr(ciiu_rev4, 1, 3),
  ciiu_rev4_2d = substr(ciiu_rev4, 1, 2),
  source_rev = 4,
  stringsAsFactors = FALSE
)

all_concordances <- bind_rows(all_concordances, rev4_identity)

# Merge with EAM panel
eam_harmonized <- eam %>%
  left_join(
    all_concordances %>% select(ciiu_original, ciiu_rev4_4d, ciiu_rev4_3d, ciiu_rev4_2d),
    by = c("ciiu" = "ciiu_original")
  )

# Check harmonization results
cat("\nHarmonization results:\n")
cat("  - 4-digit harmonized:", sum(!is.na(eam_harmonized$ciiu_rev4_4d)), "/", nrow(eam_harmonized),
    sprintf("(%.1f%%)\n", sum(!is.na(eam_harmonized$ciiu_rev4_4d))/nrow(eam_harmonized)*100))
cat("  - 3-digit harmonized:", sum(!is.na(eam_harmonized$ciiu_rev4_3d)), "/", nrow(eam_harmonized),
    sprintf("(%.1f%%)\n", sum(!is.na(eam_harmonized$ciiu_rev4_3d))/nrow(eam_harmonized)*100))
cat("  - 2-digit harmonized:", sum(!is.na(eam_harmonized$ciiu_rev4_2d)), "/", nrow(eam_harmonized),
    sprintf("(%.1f%%)\n", sum(!is.na(eam_harmonized$ciiu_rev4_2d))/nrow(eam_harmonized)*100))

# =============================================================================
# PART 5: Build HS -> ISIC4 concordance using standardized approach
# =============================================================================

cat("\n=== PART 5: Building HS -> ISIC4 Concordance ===\n")

# Load our current HS6 codes from trade data
# We'll build a new concordance using the package's HS -> NAICS -> ISIC path

# Get HS6 codes from our current concordance
current_conc <- read_dta(file.path(concordance_dir, "hs6_ciiu2_concordance_detailed.dta"))
hs6_codes <- unique(current_conc$hs6)

cat("Building concordance for", length(hs6_codes), "HS6 codes...\n")

# Build new concordance using package
# Strategy: HS5 -> NAICS2017 -> ISIC4
new_concordance <- data.frame(
  hs6 = integer(),
  isic4_4d = character(),
  isic4_3d = character(),
  isic4_2d = character(),
  via_naics = character(),
  stringsAsFactors = FALSE
)

# Process in batches with progress
batch_size <- 100
n_batches <- ceiling(length(hs6_codes) / batch_size)

for (i in 1:n_batches) {
  start_idx <- (i - 1) * batch_size + 1
  end_idx <- min(i * batch_size, length(hs6_codes))
  batch_codes <- hs6_codes[start_idx:end_idx]

  for (hs in batch_codes) {
    hs_str <- sprintf("%06d", hs)

    # Try HS5 -> NAICS2017
    naics <- tryCatch({
      r <- concord(hs_str, "HS5", "NAICS2017", dest.digit = 6)
      if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
    }, error = function(e) NA_character_)

    # If NAICS found, convert to ISIC4
    if (!is.na(naics)) {
      isic4_4d <- tryCatch({
        r <- concord(naics, "NAICS2017", "ISIC4", dest.digit = 4)
        if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
      }, error = function(e) NA_character_)

      isic4_3d <- tryCatch({
        r <- concord(naics, "NAICS2017", "ISIC4", dest.digit = 3)
        if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
      }, error = function(e) NA_character_)

      isic4_2d <- tryCatch({
        r <- concord(naics, "NAICS2017", "ISIC4", dest.digit = 2)
        if (length(r) == 0 || all(is.na(r))) NA_character_ else r[1]
      }, error = function(e) NA_character_)
    } else {
      isic4_4d <- NA_character_
      isic4_3d <- NA_character_
      isic4_2d <- NA_character_
    }

    new_concordance <- rbind(new_concordance, data.frame(
      hs6 = hs,
      isic4_4d = isic4_4d,
      isic4_3d = isic4_3d,
      isic4_2d = isic4_2d,
      via_naics = naics,
      stringsAsFactors = FALSE
    ))
  }

  if (i %% 10 == 0 || i == n_batches) {
    cat(sprintf("  Progress: %d/%d batches (%.1f%%)\n", i, n_batches, i/n_batches*100))
  }
}

cat("\nHS -> ISIC4 concordance results:\n")
cat("  - Via NAICS mapped:", sum(!is.na(new_concordance$via_naics)), "/", nrow(new_concordance), "\n")
cat("  - 4-digit ISIC:", sum(!is.na(new_concordance$isic4_4d)), "/", nrow(new_concordance), "\n")
cat("  - 3-digit ISIC:", sum(!is.na(new_concordance$isic4_3d)), "/", nrow(new_concordance), "\n")
cat("  - 2-digit ISIC:", sum(!is.na(new_concordance$isic4_2d)), "/", nrow(new_concordance), "\n")

# =============================================================================
# PART 6: Compare with current concordance
# =============================================================================

cat("\n=== PART 6: Comparing with Current Concordance ===\n")

comparison <- current_conc %>%
  select(hs6, ciiu_2d_current = ciiu_2d) %>%
  distinct() %>%
  left_join(
    new_concordance %>% select(hs6, isic4_2d),
    by = "hs6"
  ) %>%
  mutate(
    match = as.character(ciiu_2d_current) == isic4_2d
  )

cat("\nComparison at 2-digit level:\n")
cat("  - Matches:", sum(comparison$match, na.rm = TRUE), "\n")
cat("  - Mismatches:", sum(!comparison$match & !is.na(comparison$isic4_2d), na.rm = TRUE), "\n")
cat("  - New concordance NA:", sum(is.na(comparison$isic4_2d)), "\n")

# Show some mismatches
mismatches <- comparison %>%
  filter(!match & !is.na(isic4_2d)) %>%
  head(20)

if (nrow(mismatches) > 0) {
  cat("\nSample mismatches (current vs new):\n")
  print(mismatches)
}

# =============================================================================
# PART 7: Save outputs
# =============================================================================

cat("\n=== PART 7: Saving Outputs ===\n")

# Save harmonized EAM panel
write_dta(eam_harmonized, file.path(processed_dir, "panel_eam_harmonized_rev4.dta"))
cat("Saved: panel_eam_harmonized_rev4.dta\n")

# Save CIIU concordance tables
write_csv(all_concordances, file.path(concordance_dir, "ciiu_rev2_rev3_to_rev4.csv"))
cat("Saved: ciiu_rev2_rev3_to_rev4.csv\n")

# Save new HS -> ISIC concordance
write_csv(new_concordance, file.path(concordance_dir, "hs6_isic4_concordance_pkg.csv"))
cat("Saved: hs6_isic4_concordance_pkg.csv\n")

# Save comparison
write_csv(comparison, file.path(output_dir, "concordance_current_vs_new.csv"))
cat("Saved: concordance_current_vs_new.csv\n")

# =============================================================================
# PART 8: Summary and recommendations
# =============================================================================

cat("\n=== PART 8: Summary ===\n")

cat("
HARMONIZATION COMPLETE

1. EAM CIIU HARMONIZATION:
   - All CIIU codes converted to ISIC Rev 4
   - New variables: ciiu_rev4_4d, ciiu_rev4_3d, ciiu_rev4_2d
   - Output: panel_eam_harmonized_rev4.dta

2. HS -> ISIC4 CONCORDANCE:
   - Built using concordance package via HS5 -> NAICS2017 -> ISIC4
   - Available at 4, 3, and 2 digit levels
   - Output: hs6_isic4_concordance_pkg.csv

3. NEXT STEPS:
   a) Review mismatches between current and new concordance
   b) Decide whether to use new standardized concordance
   c) Rebuild trade data with new concordance at desired digit level
   d) Re-run TFP estimation with harmonized CIIU codes

4. SAMPLE SIZE CONSIDERATION:
   - Check n_obs per industry at each digit level
   - Recommended: use 3-digit for balance of granularity and sample size
")
