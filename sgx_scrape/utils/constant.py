VALID_PAIRS = frozenset(
    {
        # sector - sub_sector
        ("Basic Materials", "Metals & Mining"),
        ("Basic Materials", "Chemicals"),
        ("Basic Materials", "Building Materials"),
        ("Basic Materials", "Paper & Forest Products"),
        ("Basic Materials", "Basic Materials - Diversified"),
        ("Communication Services", "Media & Entertainment"),
        ("Communication Services", "Telecom Services"),
        ("Consumer Cyclicals", "Automotive"),
        ("Consumer Cyclicals", "Retail"),
        ("Consumer Cyclicals", "Restaurants & Food Retail"),
        ("Consumer Cyclicals", "Hotels, Resorts & Leisure"),
        ("Consumer Cyclicals", "Furnishings, Fixtures & Appliances"),
        ("Consumer Cyclicals", "Luxury Goods"),
        ("Consumer Cyclicals", "Packaging & Containers"),
        ("Consumer Cyclicals", "Apparel & Accessories"),
        # ("Consumer Cyclicals", "Media & Entertainment"),
        ("Consumer Cyclicals", "Personal Services"),
        ("Consumer Defensive", "Food & Beverage Production"),
        ("Consumer Defensive", "Food Distribution"),
        ("Consumer Defensive", "Education & Training Services"),
        ("Consumer Non-Cyclicals", "Food & Beverage Retail & Production"),
        ("Energy", "Oil & Gas Operations & Services"),
        ("Energy", "Oil, Gas & Coal"),
        ("Financial Services", "Asset Management & Investment"),
        ("Financial Services", "Banks & Credit Services"),
        ("Financial Services", "Financial Data & Shell Companies"),
        ("Financial Services", "Insurance"),
        ("Healthcare", "Healthcare Facilities & Services"),
        ("Healthcare", "Pharmaceuticals & Research"),
        ("Healthcare", "Medical Devices & Supplies"),
        ("Industrials", "Engineering, Construction & Building Products"),
        ("Industrials", "Industrial Machinery & Parts"),
        # ("Industrials", "Transportation & Logistics"),
        ("Industrials", "Supply Chain, Freight & Infrastructure"),
        ("Industrials", "Business & Employment Services"),
        ("Industrials", "Conglomerates & Holdings"),
        ("Industrials", "Aerospace & Defense"),
        ("Industrials", "Waste Management & Pollution Control"),
        ("Industrials", "Rental, Leasing & Security Services"),
        ("Industrials", "Industrial Distribution"),
        ("Infrastructures", "Heavy Construction & Transport Infrastructure"),
        ("Infrastructures", "Telecommunication"),
        ("Properties & Real Estate", "Real Estate - Development"),
        ("Properties & Real Estate", "Real Estate Services"),
        ("Properties & Real Estate", "Real Estate - Diversified"),
        ("Properties & Real Estate", "Other Real Estate Operations"),
        ("REIT", "REIT - Industrial"),
        ("REIT", "REIT - Office"),
        ("REIT", "REIT - Diversified"),
        ("REIT", "REIT - Retail"),
        ("REIT", "REIT - Healthcare"),
        ("REIT", "REIT - Hospitality"),
        ("REIT", "REIT - Data Centre"),
        ("Technology", "Hardware & Electronics"),
        ("Technology", "Software & IT Services"),
        ("Technology", "Semiconductors & Equipment"),
        ("Technology", "Communication & Instruments"),
        ("Transportation & Logistics", "Transportation & Logistics"),
        ("Utilities", "Utilities - Regulated"),
        ("Utilities", "Utilities - Renewable"),
        ("Utilities", "Utilities - Independent & Diversified"),
    }
)


# S-REIT sub-sector per REITAS (https://www.reitas.sg/singapore-reits/s-reit-sectors/).
# REITAS classifies by investment mandate, which SGX's industryName does not encode
# (e.g. Keppel DC and Digital Core are both data centre REITs but report differently),
# so these are keyed by symbol and take precedence over the string maps below.
SG_REIT_OVERRIDES = {
    # Office
    "K71U": "REIT - Office",        # Keppel REIT
    "OXMU": "REIT - Office",        # Prime US REIT
    "CMOU": "REIT - Office",        # Keppel Pacific Oak US REIT
    "BTOU": "REIT - Office",        # Manulife US REIT
    "MXNU": "REIT - Office",        # Elite UK REIT (GBP counter)
    "MENU": "REIT - Office",        # Elite UK REIT (SGD counter)
    # Industrial
    "A17U": "REIT - Industrial",    # CapitaLand Ascendas REIT
    "M44U": "REIT - Industrial",    # Mapletree Logistics Trust
    "ME8U": "REIT - Industrial",    # Mapletree Industrial Trust
    "9A4U": "REIT - Industrial",    # ESR-REIT
    "J91U": "REIT - Industrial",    # ESR-REIT (delisted counter)
    "O5RU": "REIT - Industrial",    # AIMS APAC REIT
    "UIBU": "REIT - Industrial",    # UI Boustead REIT
    "M1GU": "REIT - Industrial",    # Alpha Integrated REIT (ex-Sabana)
    "DHLU": "REIT - Industrial",    # Daiwa House Logistics Trust
    "BWCU": "REIT - Industrial",    # EC World REIT (suspended)
    # Retail
    "J69U": "REIT - Retail",        # Frasers Centrepoint Trust
    "P40U": "REIT - Retail",        # Starhill Global REIT
    "CRPU": "REIT - Retail",        # Sasseur REIT
    "ODBU": "REIT - Retail",        # United Hampshire US REIT
    "BMGU": "REIT - Retail",        # BHG Retail REIT
    "D5IU": "REIT - Retail",        # Landmark REIT (ex-Lippo Malls Indonesia)
    "SK6U": "REIT - Retail",        # Paragon REIT (delisted)
    # Healthcare
    "C2PU": "REIT - Healthcare",    # Parkway Life REIT
    "AW9U": "REIT - Healthcare",    # First REIT
    # Hospitality / Lodging
    "HMN": "REIT - Hospitality",    # CapitaLand Ascott Trust
    "8C8U": "REIT - Hospitality",   # Centurion Accommodation REIT
    "J85": "REIT - Hospitality",    # CDL Hospitality Trusts
    "Q5T": "REIT - Hospitality",    # Far East Hospitality Trust
    "XZL": "REIT - Hospitality",    # Acrophyte Hospitality Trust
    "LIW": "REIT - Hospitality",    # Eagle Hospitality Trust (suspended)
    "ACV": "REIT - Hospitality",    # Frasers Hospitality Trust (delisted)
    # Data Centre
    "AJBU": "REIT - Data Centre",   # Keppel DC REIT
    "NTDU": "REIT - Data Centre",   # NTT DC REIT
    "DCRU": "REIT - Data Centre",   # Digital Core REIT
    # Diversified
    "C38U": "REIT - Diversified",   # CapitaLand Integrated Commercial Trust
    "N2IU": "REIT - Diversified",   # Mapletree Pan Asia Commercial Trust
    "T82U": "REIT - Diversified",   # Suntec REIT
    "BUOU": "REIT - Diversified",   # Frasers Logistics & Commercial Trust
    "TS0U": "REIT - Diversified",   # OUE REIT
    "AU8U": "REIT - Diversified",   # CapitaLand China Trust
    "JYEU": "REIT - Diversified",   # Lendlease Global Commercial REIT
    "SEB": "REIT - Diversified",    # Stoneweg Europe Stapled Trust (SGD counter)
    "SET": "REIT - Diversified",    # Stoneweg Europe Stapled Trust (EUR counter)
    "CWBU": "REIT - Diversified",   # Stoneweg European REIT (delisted counter)
    "CWCU": "REIT - Diversified",   # Stoneweg Europe Stapled Trust (delisted counter)
    "UD1U": "REIT - Diversified",   # IREIT Global
    "8U7U": "REIT - Diversified",   # IREIT Global (secondary counter)
}


SECTOR_REMAP = {
    "Financials": "Financial Services",
    "Transportation & Logistic": "Transportation & Logistics",
}


DIRECT_SUBSECTOR_MAP = {
    "Medical Distribution": ("Healthcare", "Medical Devices & Supplies"),
    "Oil, Gas & Coal": ("Energy", "Oil, Gas & Coal"),
    "Utilities - Diversified": ("Utilities", "Utilities - Independent & Diversified"),
    "Electronic Components": ("Technology", "Hardware & Electronics"),
    "REIT - Diversified": ("REIT", "REIT - Diversified"),
    "Software - Infrastructure": ("Technology", "Software & IT Services"),
    "Software - Application": ("Technology", "Software & IT Services"),
    "Electrical Equipment & Parts": ("Industrials", "Industrial Machinery & Parts"),
    "Insurance": ("Financial Services", "Insurance"),
    "Pharmaceutical Retailers": ("Healthcare", "Healthcare Facilities & Services"),
    "Internet Retail": ("Consumer Cyclicals", "Retail"),
    "Leisure": ("Consumer Cyclicals", "Hotels, Resorts & Leisure"),
    "Education & Training Services": ("Consumer Defensive", "Education & Training Services"),
    "Medical Devices": ("Healthcare", "Medical Devices & Supplies"),
    "Confectioners": ("Consumer Defensive", "Food & Beverage Production"),
    "Auto & Truck Dealerships": ("Consumer Cyclicals", "Automotive"),
    "Industrial Machinery & Equipment": ("Industrials", "Industrial Machinery & Parts"),
    "Paper & Paper Products": ("Basic Materials", "Paper & Forest Products"),
    "Capital Markets": ("Financial Services", "Asset Management & Investment"),
    "REIT - Industrial": ("REIT", "REIT - Industrial"),
    "Utilities - Independent Power Producers": ("Utilities", "Utilities - Independent & Diversified"),
    "Specialty Industrial Machinery": ("Industrials", "Industrial Machinery & Parts"),
    "Asset Management": ("Financial Services", "Asset Management & Investment"),
    "Automobiles & Components": ("Consumer Cyclicals", "Automotive"),
    "Resorts & Casinos": ("Consumer Cyclicals", "Hotels, Resorts & Leisure"),
    "Financial Data & Stock Exchanges": ("Financial Services", "Financial Data & Shell Companies"),
    "Lumber & Wood Production": ("Basic Materials", "Paper & Forest Products"),
    "Banks": ("Financial Services", "Banks & Credit Services"),
    "Medical Instruments & Supplies": ("Healthcare", "Medical Devices & Supplies"),
    "Auto Manufacturers": ("Consumer Cyclicals", "Automotive"),
    "Thermal Coal": ("Energy", "Oil, Gas & Coal"),
    "Furnishings, Fixtures & Appliances": ("Consumer Cyclicals", "Furnishings, Fixtures & Appliances"),
    "Utilities - Regulated Gas": ("Utilities", "Utilities - Regulated"),
    "Specialty Business Services": ("Industrials", "Business & Employment Services"),
    "Specialty Chemicals": ("Basic Materials", "Chemicals"),
    "Insurance - Property & Casualty": ("Financial Services", "Insurance"),
    "Other Industrial Metals & Mining": ("Basic Materials", "Metals & Mining"),
    "REIT - Retail": ("REIT", "REIT - Retail"),
    "Credit Services": ("Financial Services", "Banks & Credit Services"),
    "Multi-sector Holdings": ("Industrials", "Conglomerates & Holdings"),
    "Packaging & Containers": ("Consumer Cyclicals", "Packaging & Containers"),
    "Real Estate - Diversified": ("Properties & Real Estate", "Real Estate - Diversified"),
    "Medical Care Facilities": ("Healthcare", "Healthcare Facilities & Services"),
    "Real Estate Services": ("Properties & Real Estate", "Real Estate Services"),
    "Utilities - Regulated Water": ("Utilities", "Utilities - Regulated"),
    "Luxury Goods": ("Consumer Cyclicals", "Luxury Goods"),
    "Oil & Gas E&P": ("Energy", "Oil & Gas Operations & Services"),
    "Department Stores": ("Consumer Cyclicals", "Retail"),
    "Staffing & Employment Services": ("Industrials", "Business & Employment Services"),
    "Specialty Retail": ("Consumer Cyclicals", "Retail"),
    "Real Estate - Development": ("Properties & Real Estate", "Real Estate - Development"),
    "Engineering & Construction": ("Industrials", "Engineering, Construction & Building Products"),
    "REIT - Office": ("REIT", "REIT - Office"),
    "Apparel Manufacturing": ("Consumer Cyclicals", "Apparel & Accessories"),
    "Steel": ("Basic Materials", "Metals & Mining"),
    "Restaurants & Bars": ("Consumer Cyclicals", "Restaurants & Food Retail"),
    "Conglomerates": ("Industrials", "Conglomerates & Holdings"),
    "Lodging": ("Consumer Cyclicals", "Hotels, Resorts & Leisure"),
    "Chemicals": ("Basic Materials", "Chemicals"),
    "Metal Fabrication": ("Industrials", "Industrial Machinery & Parts"),
    "Diagnostics & Research": ("Healthcare", "Pharmaceuticals & Research"),
    "Oil & Gas Midstream": ("Energy", "Oil & Gas Operations & Services"),
    "Building Products & Equipment": ("Industrials", "Engineering, Construction & Building Products"),
    "Computer Hardware": ("Technology", "Hardware & Electronics"),
    "Apparel Retail": ("Consumer Cyclicals", "Apparel & Accessories"),
    "Restaurants": ("Consumer Cyclicals", "Restaurants & Food Retail"),
    "Personal Services": ("Consumer Cyclicals", "Personal Services"),
    "Waste Management": ("Industrials", "Waste Management & Pollution Control"),
    "Health Information Services": ("Healthcare", "Healthcare Facilities & Services"),
    "Business Equipment & Supplies": ("Industrials", "Business & Employment Services"),
    "Pollution & Treatment Controls": ("Industrials", "Waste Management & Pollution Control"),
    "Integrated Oil": ("Energy", "Oil & Gas Operations & Services"),
    "Oil & Gas Refining & Marketing": ("Energy", "Oil & Gas Operations & Services"),
    "REIT - Specialty": ("REIT", "REIT - Diversified"),
    "REIT - Hotel & Motel": ("REIT", "REIT - Hospitality"),
    "Packaged Foods": ("Consumer Defensive", "Food & Beverage Production"),
    "REIT - Healthcare Facilities": ("REIT", "REIT - Healthcare"),
    "Utilities - Renewable": ("Utilities", "Utilities - Renewable"),
    "Pharmaceuticals & Health Care Research": ("Healthcare", "Pharmaceuticals & Research"),
    "Gold": ("Basic Materials", "Metals & Mining"),
    "Building Materials": ("Basic Materials", "Building Materials"),
    "Security & Protection Services": ("Industrials", "Rental, Leasing & Security Services"),
    "Auto Parts": ("Consumer Cyclicals", "Automotive"),
    "Technology Hardware & Equipment": ("Technology", "Hardware & Electronics"),
    "Mortgage Finance": ("Financial Services", "Banks & Credit Services"),
    "Shell Companies": ("Financial Services", "Financial Data & Shell Companies"),
    "Farm Products": ("Consumer Defensive", "Food & Beverage Production"),
    "Communication Equipment": ("Technology", "Communication & Instruments"),
    "Oil & Gas Equipment & Services": ("Energy", "Oil & Gas Operations & Services"),
    "Aerospace & Defense": ("Industrials", "Aerospace & Defense"),
    "Investment Service": ("Financial Services", "Asset Management & Investment"),
    "Drug Manufacturers - Specialty & Generic": ("Healthcare", "Pharmaceuticals & Research"),
    "Industrial Distribution": ("Industrials", "Industrial Distribution"),
    "Aluminum": ("Basic Materials", "Metals & Mining"),
    "Information Technology Services": ("Technology", "Software & IT Services"),
    "Beverages - Non-Alcoholic": ("Consumer Defensive", "Food & Beverage Production"),
    "Consulting Services": ("Industrials", "Business & Employment Services"),
    "Rental & Leasing Services": ("Industrials", "Rental, Leasing & Security Services"),
    "Semiconductor Equipment & Materials": ("Technology", "Semiconductors & Equipment"),
    "Healthcare Equipment & Providers": ("Healthcare", "Healthcare Facilities & Services"),
    "Semiconductors": ("Technology", "Semiconductors & Equipment"),
    "Apparel & Accessories Retailers": ("Consumer Cyclicals", "Apparel & Accessories"),
    "Marine Fishing & Aquaculture": ("Consumer Defensive", "Food & Beverage Production"),
    "Seafood Sourcing & Distribution": ("Consumer Defensive", "Food Distribution"),
    "Hospitality REITs (Hotels)": ("REIT", "REIT - Hospitality"),
    "Communications & Networking Infrastructure": ("Technology", "Communication & Instruments"),
    "Shipbuilding & Marine Engineering": ("Industrials", "Industrial Machinery & Parts"),
    "Offshore Oil & Gas Services": ("Energy", "Oil & Gas Operations & Services"),
    "Furniture Manufacturing & Retail": ("Consumer Cyclicals", "Furnishings, Fixtures & Appliances"),
    "Real Estate Management & Development": ("Properties & Real Estate", "Real Estate - Development"),
}


SECONDARY_PAIR_MAP = {
    # Media & Entertainment family: target sub stays "Media & Entertainment",
    ("Communication Services", "Entertainment"): ("Communication Services", "Media & Entertainment"),
    ("Consumer Cyclicals", "Entertainment"): ("Consumer Cyclicals", "Hotels, Resorts & Leisure"),
    ("Communication Services", "Publishing"): ("Communication Services", "Media & Entertainment"),
    ("Consumer Cyclicals", "Publishing"): ("Consumer Cyclicals", "Hotels, Resorts & Leisure"),
    ("Communication Services", "Advertising Agencies"): ("Communication Services", "Media & Entertainment"),
    ("Consumer Cyclicals", "Advertising Agencies"): ("Consumer Cyclicals", "Hotels, Resorts & Leisure"),
    ("Communication Services", "Electronic Gaming & Multimedia"): ("Communication Services", "Media & Entertainment"),
    ("Consumer Cyclicals", "Electronic Gaming & Multimedia"): ("Consumer Cyclicals", "Hotels, Resorts & Leisure"),

    # Telecom family.
    ("Infrastructures", "Telecom Services"): ("Infrastructures", "Telecommunication"),
    ("Communication Services", "Telecommunication"): ("Communication Services", "Telecom Services"),
    # ("Communication Services", "Telecom Services") and
    # ("Infrastructures", "Telecommunication") are already valid pairs.
 
    # Transportation: target sub is "Transportation & Logistics",
    # sector stays Industrials or the standalone Transportation & Logistics.
    ("Industrials", "Transportation"): ("Industrials", "Supply Chain, Freight & Infrastructure"),
    ("Transportation & Logistics", "Transportation"): ("Transportation & Logistics", "Transportation & Logistics"),
    ("Industrials", "Other Transportation"): ("Industrials", "Supply Chain, Freight & Infrastructure"),
    ("Transportation & Logistics", "Other Transportation"): ("Transportation & Logistics", "Transportation & Logistics"),
    ("Industrials", "Marine Shipping"): ("Industrials", "Supply Chain, Freight & Infrastructure"),
    ("Transportation & Logistics", "Marine Shipping"): ("Transportation & Logistics", "Transportation & Logistics"),
    ("Industrials", "Railroads"): ("Industrials", "Supply Chain, Freight & Infrastructure"),
    ("Transportation & Logistics", "Railroads"): ("Transportation & Logistics", "Transportation & Logistics"),
    ("Industrials", "Integrated Freight & Logistics"): ("Industrials", "Supply Chain, Freight & Infrastructure"),
    ("Transportation & Logistics", "Integrated Freight & Logistics"): ("Transportation & Logistics", "Transportation & Logistics"),
    ("Industrials", "Logistics & Deliveries"): ("Industrials", "Supply Chain, Freight & Infrastructure"),
    ("Transportation & Logistics", "Logistics & Deliveries"): ("Transportation & Logistics", "Transportation & Logistics"),

    # Transportation Infrastructure: Infrastructures vs Industrials/Transport.
     ("Infrastructures", "Transportation Infrastructure"): ("Infrastructures", "Heavy Construction & Transport Infrastructure"),
    ("Industrials", "Transportation Infrastructure"): ("Industrials", "Supply Chain, Freight & Infrastructure"),
    ("Transportation & Logistics", "Transportation Infrastructure"): ("Transportation & Logistics", "Transportation & Logistics"),
 
    # Heavy Constructions & Civil Engineering.
    ("Industrials", "Heavy Constructions & Civil Engineering"): ("Industrials", "Engineering, Construction & Building Products"),
    ("Infrastructures", "Heavy Constructions & Civil Engineering"): ("Infrastructures", "Heavy Construction & Transport Infrastructure"),
 
    # Infrastructure Operations (only Infrastructures sub that fits).
    ("Infrastructures", "Infrastructure Operations"): ("Infrastructures", "Heavy Construction & Transport Infrastructure"),
 
    # Food. Consumer Defensive vs Consumer Non-Cyclicals.
    ("Consumer Defensive", "Food & Beverage"): ("Consumer Defensive", "Food & Beverage Production"),
    ("Consumer Non-Cyclicals", "Food & Beverage"): ("Consumer Non-Cyclicals", "Food & Beverage Retail & Production"),
    ("Consumer Non-Cyclicals", "Food Distribution"): ("Consumer Non-Cyclicals", "Food & Beverage Retail & Production"),
    ("Consumer Defensive", "Food & Staples Retailing"): ("Consumer Defensive", "Food Distribution"),
    ("Consumer Non-Cyclicals", "Food & Staples Retailing"): ("Consumer Non-Cyclicals", "Food & Beverage Retail & Production"),
    # ("Consumer Defensive", "Food Distribution") already valid.
 
    # Investment Holding Companies: Financial vs Industrials.
    ("Financial Services", "Investment Holding Companies"): ("Financial Services", "Asset Management & Investment"),
    ("Industrials", "Investment Holding Companies"): ("Industrials", "Conglomerates & Holdings"),
 
    # Scientific & Technical Instruments: Technology vs Healthcare.
    ("Technology", "Scientific & Technical Instruments"): ("Technology", "Communication & Instruments"),
    ("Healthcare", "Scientific & Technical Instruments"): ("Healthcare", "Medical Devices & Supplies"),
 
    # Electronics & Computer Distribution: Technology vs Industrials.
    ("Technology", "Electronics & Computer Distribution"): ("Technology", "Hardware & Electronics"),
    ("Industrials", "Electronics & Computer Distribution"): ("Industrials", "Industrial Distribution"),
 
    # Farm & Heavy Construction Machinery: default to Industrial Machinery.
    ("Industrials", "Farm & Heavy Construction Machinery"): ("Industrials", "Industrial Machinery & Parts"),
 
    # Tools & Accessories: default to Industrial Machinery when Industrials.
    ("Industrials", "Tools & Accessories"): ("Industrials", "Industrial Machinery & Parts"),

    ("REIT", "REIT - Hotel & Motel"): ("REIT", "REIT - Hospitality"),
    ("Basic Materials", "Basic Materials"): ("Basic Materials", "Basic Materials - Diversified")
}