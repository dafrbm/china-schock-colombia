# =============================================================================
# Exploration of R concordance package capabilities
# Goal: Evaluate if we can build better concordances at higher disaggregation
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
# 1. Explore concordance package capabilities
# =============================================================================

cat("\n=== Concordance Package Capabilities ===\n")

# Check available concordance tables
cat("\nAvailable origin codes:\n")
# HS revisions: HS0 (1988), HS1 (1996), HS2 (2002), HS3 (2007), HS4 (2012), HS5 (2017)
# SITC revisions: SITC1, SITC2, SITC3, SITC4
# ISIC revisions: ISIC2, ISIC3, ISIC3.1, ISIC4
# NAICS: NAICS2002, NAICS2007, NAICS2012, NAICS2017

cat("HS revisions: HS0 (1988), HS1 (1996), HS2 (2002), HS3 (2007), HS4 (2012), HS5 (2017)\n")
cat("ISIC revisions: ISIC2, ISIC3, ISIC3.1, ISIC4\n")
cat("NAICS: NAICS2002, NAICS2007, NAICS2012, NAICS2017\n")

# =============================================================================
# 2. Test direct HS -> ISIC mapping at different digit levels
# =============================================================================

cat("\n=== Testing HS -> ISIC Mappings ===\n")

# Sample HS6 codes (manufacturing products)
test_codes <- c("847130", "854231", "300490", "940360", "611030", "840820")

cat("\nDirect HS5 -> ISIC4 mappings:\n")
for (hs in test_codes) {
  # Test at different ISIC digit levels
  isic2 <- tryCatch(concord(hs, "HS5", "ISIC4", dest.digit = 2), error = function(e) NA)
  isic3 <- tryCatch(concord(hs, "HS5", "ISIC4", dest.digit = 3), error = function(e) NA)
  isic4 <- tryCatch(concord(hs, "HS5", "ISIC4", dest.digit = 4), error = function(e) NA)

  cat(sprintf("HS %s -> ISIC2: %s, ISIC3: %s, ISIC4: %s\n",
              hs,
              ifelse(length(isic2) == 0 || is.na(isic2[1]), "NA", paste(isic2, collapse=",")),
              ifelse(length(isic3) == 0 || is.na(isic3[1]), "NA", paste(isic3, collapse=",")),
              ifelse(length(isic4) == 0 || is.na(isic4[1]), "NA", paste(isic4, collapse=","))))
}

# =============================================================================
# 3. Compare with our current concordance
# =============================================================================

cat("\n=== Comparing with Our Concordance ===\n")

# Load our concordance
our_conc <- read_dta(file.path(concordance_dir, "hs6_ciiu2_concordance_detailed.dta"))

# Get unique HS6 codes
our_hs6 <- unique(our_conc$hs6)
cat("Our concordance has", length(our_hs6), "unique HS6 codes\n")

# Test a sample against the package
set.seed(123)
sample_hs6 <- sample(our_hs6, min(100, length(our_hs6)))

comparison <- data.frame(
  hs6 = character(),
  our_ciiu2 = character(),
  pkg_isic2 = character(),
  match = logical(),
  stringsAsFactors = FALSE
)

for (hs in sample_hs6) {
  hs_str <- sprintf("%06d", hs)

  # Our mapping
  our_ciiu <- our_conc %>%
    filter(hs6 == hs) %>%
    pull(ciiu_2d) %>%
    unique() %>%
    sort() %>%
    paste(collapse = ",")

  # Package mapping
  pkg_isic <- tryCatch({
    result <- concord(hs_str, "HS5", "ISIC4", dest.digit = 2)
    if (length(result) == 0 || all(is.na(result))) "NA" else paste(sort(result), collapse = ",")
  }, error = function(e) "NA")

  comparison <- rbind(comparison, data.frame(
    hs6 = hs_str,
    our_ciiu2 = our_ciiu,
    pkg_isic2 = pkg_isic,
    match = (our_ciiu == pkg_isic),
    stringsAsFactors = FALSE
  ))
}

cat("\nComparison results (sample of 100 HS6 codes):\n")
cat("Matches:", sum(comparison$match, na.rm = TRUE), "\n")
cat("Mismatches:", sum(!comparison$match & comparison$pkg_isic2 != "NA", na.rm = TRUE), "\n")
cat("Package returned NA:", sum(comparison$pkg_isic2 == "NA"), "\n")

# Show some mismatches
mismatches <- comparison %>%
  filter(!match & pkg_isic2 != "NA")

if (nrow(mismatches) > 0) {
  cat("\nSample mismatches:\n")
  print(head(mismatches, 10))
}

# =============================================================================
# 4. Check leading zeros handling in EAM
# =============================================================================

cat("\n=== Leading Zeros Analysis ===\n")

# Load EAM panel to check CIIU codes
eam_panel <- tryCatch({
  read_dta(file.path(processed_dir, "panel_eam_1992_2023.dta"))
}, error = function(e) {
  cat("Could not load EAM panel:", e$message, "\n")
  NULL
})

if (!is.null(eam_panel)) {
  # Check ciiu_2d values
  cat("\nEAM ciiu_2d unique values:\n")
  eam_ciiu <- sort(unique(eam_panel$ciiu_2d))
  cat(paste(eam_ciiu, collapse = ", "), "\n")

  # Check for potential leading zero issues
  # In numeric format, 10 and "10" are the same, but if stored as string "010" would differ
  cat("\nciiu_2d type:", class(eam_panel$ciiu_2d), "\n")
  cat("Range:", range(eam_panel$ciiu_2d, na.rm = TRUE), "\n")

  # Check original ciiu variable if available
  if ("ciiu" %in% names(eam_panel)) {
    cat("\nOriginal ciiu variable:\n")
    cat("Type:", class(eam_panel$ciiu), "\n")
    # Sample values
    cat("Sample values:", head(unique(eam_panel$ciiu), 20), "\n")
  }
}

# =============================================================================
# 5. Explore higher disaggregation potential
# =============================================================================

cat("\n=== Higher Disaggregation Analysis ===\n")

# Check if we can map to ISIC 3-digit or 4-digit
# This requires checking:
# 1. Package coverage at 3/4 digit level
# 2. EAM data availability at 3/4 digit level

# Test coverage at 3-digit level
test_3digit <- sapply(sample_hs6[1:20], function(hs) {
  hs_str <- sprintf("%06d", hs)
  result <- tryCatch(concord(hs_str, "HS5", "ISIC4", dest.digit = 3), error = function(e) NA)
  if (length(result) == 0 || all(is.na(result))) NA else result[1]
})

cat("\n3-digit ISIC coverage (sample of 20):\n")
cat("Mapped:", sum(!is.na(test_3digit)), "/ 20\n")

# Check EAM 3-digit availability
if (!is.null(eam_panel) && "ciiu_3d" %in% names(eam_panel)) {
  eam_ciiu3 <- sort(unique(eam_panel$ciiu_3d[!is.na(eam_panel$ciiu_3d)]))
  cat("\nEAM has", length(eam_ciiu3), "unique CIIU 3-digit codes\n")
  cat("Sample:", head(eam_ciiu3, 20), "\n")
}

# =============================================================================
# 6. Recommendations
# =============================================================================

cat("\n=== Recommendations ===\n")

cat("
Based on this exploration:

1. LEADING ZEROS:
   - If ciiu_2d is numeric in EAM, no leading zero issue at 2-digit level
   - At 3+ digits, need to verify string vs numeric handling

2. CONCORDANCE VALIDATION:
   - Our concordance uses Pierce & Schott (HS->NAICS) + NAICS->ISIC
   - Package uses direct HS->ISIC tables
   - Differences are expected but should be minor

3. HIGHER DISAGGREGATION:
   - Feasible if EAM has reliable 3-digit or 4-digit CIIU
   - Trade data from Comtrade is at HS6, maps well to ISIC 3-4 digit
   - Main constraint: sample size per industry for TFP estimation

4. RECOMMENDED NEXT STEPS:
   a) Verify EAM ciiu_3d/ciiu_4d quality and coverage
   b) Build concordance at 3-digit level using package
   c) Compare results with 2-digit analysis for robustness
")

# Save comparison results
write_csv(comparison, file.path(output_dir, "concordance_comparison.csv"))
cat("\nComparison saved to: output/concordance_comparison.csv\n")
