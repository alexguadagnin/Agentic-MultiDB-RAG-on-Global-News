import psycopg2
import re
import sys
import time

# --- 1. CONFIGURAZIONE ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "gdelt_rag_db"
DB_USER = "gdelt_admin"
DB_PASS = "strong_password_123"

# Quante righe aggiornare alla volta (Batch size)
BATCH_SIZE = 2000 

# --- 2. MAPPING DATI (INCLUSO) ---

CAMEO_EVENT_MAPPING = {
    '01': 'make public statement', '02': 'appeal', '03': 'express intent to cooperate',
    '04': 'consult', '05': 'engage in diplomatic cooperation', '06': 'engage in material cooperation',
    '07': 'provide aid', '08': 'yield', '09': 'investigate', '10': 'demand',
    '11': 'disapprove', '12': 'reject', '13': 'threaten', '14': 'protest',
    '15': 'exhibit force posture', '16': 'reduce relations', '17': 'coerce',
    '18': 'assault', '19': 'fight', '20': 'use unconventional mass violence'
}

# Include tutte le regole "Platinum" (v10.0) che abbiamo definito
THEME_MAPPING_RULES = {
  "humanitarian_aid_organizations": [
    "RED_CROSS", "RED_CRESCENT", "AMNESTY_INTERNATIONAL", "OXFAM", "UNICEF",
    "MEDECINS_SANS_FRONTIERES", "SAVE_THE_CHILDREN", "CARE_INTERNATIONAL",
    "WORLD_VISION", "HUMAN_RIGHTS_WATCH", "AIDGROUPS", "REFUGEE_COUNCIL",
    "RELIEF_INTERNATIONAL", "ACTION_AGAINST_HUNGER", "PLANET_AID", 
    "WHO_", "WORLD_HEALTH_ORGANIZATION", "NON_GOVERNMENTAL_ORGANIZATIONS",
    "SHELTER", "FIELDHOSPITAL", "DISTRESSSIGNAL", "EMERGENCY_CONDITIONS",
    "SUPPORT_SERVICES", "FOOD_DISTRIBUTION", "EMERGENCY_PREPAREDNESS",
    "HUMANITARIAN", "CONTINGENCY_PLANNING", "FUNERAL_GRANTS", "MASSCASUALTY",
    "DISASTER_PREPAREDNESS", "DISASTER_RISK", "RESILIENCE", "RESILIENT", "RISK",
    "HYGIENE_PROMOTION", "DIRECT_SERVICES", "REFUGEE_SUPPORT", "RETURNING_MIGRANTS",
    "COMMUNITY_OUTREACH"
  ],
  "politics_governance": [
    "POLITICS", "GOV_", "GENERAL_GOVERNMENT", "LEGISLATION", "ELECTION", 
    "DEMOCRACY", "LEADER", "USPEC_POLITICS", "WB_696_PUBLIC_SECTOR_MANAGEMENT", 
    "WB_723_PUBLIC_ADMINISTRATION", "POLICY", "USPEC_POLICY", "UNCERTAINTY", 
    "SOVEREIGNTY", "CONSTITUTIONAL", "RESIGNATION", "APPOINTMENT", "IDEOLOGY",
    "GOVERNANCE", "LOCAL_GOVERNMENT", "DICTATORSHIP", "VETO", "SCANDAL", "PROPAGANDA",
    "IMPEACHMENT", "PRIVATIZATION", "REGULATION", "STANDARDS", "ETHICS", 
    "FREEDOMS", "GRIEVANCES", "HUMAN_RESOURCES", "INSPECTIONS", "LICENSING", 
    "PERMITS", "SERVICE_DELIVERY", "ACCOUNTABILITY", "OMBUDSMEN", "STATE_OF_EMERGENCY",
    "MAKESTATEMENT", "TAKE_OFFICE", "MONITORING", "EVALUATION", "INSPECTORATE",
    "PUBLIC_OPINION", "CAMPAIGN", "WHISTLEBLOWER", "CIVIL_LIBERTIES", "AUDIT",
    "PROCUREMENT", "CAPACITY_BUILDING", "INSTITUTIONAL_REFORM", "CLAIM_CREDIT",
    "MANAGEMENT", "SIMPLIFICATION", "PERFORMANCE", "SIZE_OF_THE_PUBLIC_SERVICE",
    "DOWNSIZING", "DECENTRALIZATION", "E_GOVERNMENT", "DATA_DRIVEN", "CENSUS",
    "PUBLIC_SERVICE", "CHANGE_MANAGEMENT", "ADMINISTRATIVE", "SURVEY",
    "PARLIAMENT", "LEGISLATURES", "CIVIL_SERVANTS", "MUNICIPAL", "REGISTRIES",
    "PARTNERSHIPS", "PPP_", "PUBLIC_PRIVATE", "STRATEGIES", "AGENCY", "CODES_OF_CONDUCT",
    "CENTER_OF_GOVERNMENT", "MACHINERY_OF_GOVERNMENT", "ORGANIZATION_OF_PUBLIC",
    "IMPACT_ASSESSMENT", "PARTICIPATION", "SEPARATION_OF_FUNCTIONS", "TARGETING",
    "M_AND_E", "PPPS", "POLITICAL_ECONOMY", "THEMATIC_BASED", "PARENTS_ASSOCIATIONS",
    "PRIORTIZATION", "PROPOSALS", "RIGHT_TO_SERVICES", "QUALITY_SYSTEMS",
    "SERVICES_DELIVERY"
  ],
  "international_relations": [
    "DIPLOMACY", "NEGOTIATION", "TREATIES", "RATIFY", "ALLIANCE", "CEASEFIRE",
    "SANCTIONS", "PEACE", "RELATIONS", "FOREIGN", "SUMMIT", "INTEGRATION", "EXILE",
    "RECONCILIATION", "REPARATIONS", "YIELD", "BORDER_CLOSURE", "CLOSURE",
    "CUSTOMS_UNIONS", "LANDLOCKED", "TRANSIT_PROCEDURES", "HOSTVISIT", "RECOGNITION",
    "MUTUAL_RECOGNITION", "BOUNDARIES", "COMMERCIAL_PRESENCE", "EXPORT_PROMOTION",
    "BALI_AGREEMENTS", "SERVICES_COMMITMENTS", "EXPORT_ZONES"
  ],
  "conflict_security_defense": [
    "ARMEDCONFLICT", "MILITARY", "SECURITY", "TERROR", "CRIME", "WEAPONS", 
    "TAX_WEAPONS", "KILL", "ARREST", "WOUND", "FRAGILITY", "VIOLENCE", 
    "ORGANIZED_CRIME", "BORDER", "REBELLION", "PROTEST", "UNREST", "EXTREMISM", 
    "KIDNAP", "EVACUATION", "ATROCITY", "REVOLUTIONARY", "DRONES", "GENOCIDE",
    "HOSTAGE", "STRIKE", "BLOCKADE", "RETALIATE", "SMUGGLING", "GANGS", "REBELS",
    "WAR", "CONFLICT", "SEIGE", "SURVEILLANCE", "ASSASSINATION", "VANDALIZE",
    "TREASON", "ARSON", "SEIZE", "WMD", "MILITIA", "DEATH_PENALTY", 
    "PREVENTION", "PIRACY", "JIHAD", "SUICIDE", "INSURGENCY", "MERCENARIES", 
    "CHECKPOINT", "CARTELS", "FORCEPOSTURE", "ETHNIC_CLEANSING", "PARAMILITARIES",
    "SEPARATISTS", "CURFEW", "LANDMINE", "DEFECTION", "BLACK_MARKET", "EXHUMATION",
    "HATE_SPEECH", "ISLAMOPHOBIA", "BOMB", "SUSPICIOUS", "HAZMAT", "ARMS_DEAL",
    "TERRITORIAL_DEFENSE", "COMBATANTS", "SPECIALDEATH", "THREATEN", "HARM",
    "CHILD_SOLDIERS", "UNGOVERNED", "RADICALIZATION", "COUNTERFEIT", "ABDUCTION",
    "POWER_SHARING"
  ],
  "justice_law": [
    "JUSTICE", "CORRUPTION", "TRIAL", "PRISON", "DETENTION", "LAW", "LEGAL", 
    "DISPUTE", "INVESTIGATION", "BRIBERY", "FRAUD", "TRANSPARENCY", "BAN", 
    "TRADEMARKS", "PROPERTY_LAWS", "PERSECUTION", "HARASSMENT", "TORTURE",
    "CONFISCATION", "MONEY_LAUNDERING", "ANTITRUST", "PATENTS", 
    "INTELLECTUAL_PROPERTY", "ARBITRATION", "PROPERTY_RIGHTS", "ANTICARTEL",
    "CONCILIATION", "JUDICIAL", "COURT", "ENFORCEMENT", "INSOLVENCY", 
    "DOMINANCE", "MONOPOLIZATION", "OWNERSHIP", "ASSETS", "EMINENT_DOMAIN"
  ],
  "economy_trade_industry": [
    "ECON_", "EPU_", "TAX_ECON", "TRADE", "GROWTH", "PRIVATE_SECTOR", 
    "FINANCIAL", "BUSINESS", "JOBS", "AGRICULTURE", "DEBT", "INFLATION", 
    "EMPLOYMENT", "LABOR", "INDUSTRY", "COMPETITIVE", "INSURANCE", 
    "COMPENSATION", "RETIREMENT", "MANUFACTURING", "CAPITAL_MARKETS", 
    "ENTERPRISE", "EMPLOYER", "WAGES", "SUBSIDIES", "BANKING", 
    "FINANCE", "TAXATION", "MACROECONOMIC", "TREASURY", "RECRUITMENT", 
    "TRAINING", "INTERNSHIP", "APPRENTICESHIP", "COMMODITIES", "FISCAL",
    "VOLATILITY", "LEASING", "FDI", "INVESTMENT", "AUSTERITY", "DEVELOPMENT",
    "FEE", "WAIVER", "PENSION", "COLLECTIVE_BARGAINING", "OUTSOURCING", 
    "BALANCE_OF_PAYMENTS", "WORKING_CONDITIONS", "MARKET", "SECURITIES",
    "SHOCKS", "TARIFF", "EXPENDITURE", "PAYMENT", "ASSET", "CURRENT_ACCOUNT", 
    "BUDGET", "REVENUE", "CREDIT", "COST_OF_LIVING", "SUPPLY_CHAIN", "INCENTIVES",
    "MINERAL", "RETAIL", "FREIGHT", "TOLLS", "PORTFOLIO", "FREE_ZONES", "EQUITY",
    "CONTRACT", "INFORMALITY", "CORPORATE", "RETURNS_TO_WORK", "WORKING_HOURS",
    "PART_TIME", "JOB", "WORK", "ECONOMIC", "AGRICULTURAL", "AGRO_PROCESSING",
    "FARM", "LIVESTOCK", "TRANSFER_PRICING", "TRANSFER_PAYMENTS", "MUTUAL_FUNDS",
    "REMITTANCES", "CAPITAL", "SHADOW_WAGE", "COMMERCIAL", "AGGLOMERATION",
    "PRODUCTIVITY", "FLEXICURITY", "DISMISSAL", "JOB_CREATION", "JOB_SHARING",
    "JOB_ROTATION", "EARLY_STAGE", "MENTORING", "LIVING_LABS", "WEALTH_FUNDS",
    "REMITTANCES", "MONETARY", "FUNDS", "TRANSFORMATION", "CONSUMPTION", 
    "VALUE_CHAIN", "COMMERCE", "MACRO_VULNERABILITY", "PRODUCTION_FRONTIER",
    "RESERVE_RATES", "IMPACT_OF_COMPETITION"
  ],
  "resources_energy_environment": [
    "ENV_", "NATURAL_DISASTER", "CLIMATE", "WATER", "ENVIRONMENT", "ENERGY", 
    "FORESTS", "CRISISLEX", "MANMADE_DISASTER", "MINING", "GOLD", "SILVER", 
    "METAL", "ORE", "ECOSYSTEMS", "NATURAL_RESOURCE", "COPPER", "IRON", 
    "RENEWABLE", "OIL", "GAS", "FIRE", "SANITATION", "IRRIGATION", "DRAINAGE",
    "PIPELINES", "REFINERIES", "BIODIVERSITY", "POWER_SYSTEMS", "MOUNTAINS",
    "CROP", "ANIMAL_PRODUCTION", "FERTILIZER", "POLLUTION", "PEST_MANAGEMENT",
    "WETLANDS", "PARKS", "HYDROMET", "PROTECTED_AREAS", "METHANE", "PESTICIDES",
    "HYDROPOWER", "ZINC", "NICKEL", "LEVEES", "DRYLANDS", "WASTE_MANAGEMENT",
    "ANIMAL_WELFARE", "FLOOD", "HAZARDOUS_WASTE", "TUNGSTEN", "ECOSYSTEM",
    "SOLID_WASTE", "GREEN", "LAND_USE", "SPATIAL_PLANNING", "WELLS", "FRACKING",
    "WAVE_POWER", "CHEMICAL", "RECLAMATION", "GEOLOGICAL", "FUEL", "EXTRACTIVE",
    "CARBON", "LAND_REFORM", "TIN", "TANTALUM", "HABITATS", "EROSION", "SALINITY",
    "EXPLORATORY_DRILLING", "WILDLIFE", "CANALS", "HEADWORKS", "RESTORATION",
    "PASTURE", "RIPARIAN", "FACTORY_FARM", "METEOROLOGICAL", "PASTORALISM",
    "ORGANISMS", "POLLUTANTS", "GENETICALLY_MODIFIED", "WETLAND", "NATURAL_HABITAT"
  ],
  "health_society_welfare": [
    "HEALTH", "MEDICAL", "DISEASE", "NUTRITION", "PHARMACEUTICALS", "PANDEMIC", 
    "CANCER", "INJURY", "DRUG", "NARCOTICS", "SUPPLEMENTS", "HUMAN_RIGHTS", 
    "REFUGEES", "AID_", "SOC_", "SOCIAL", "POVERTY", "DISCRIMINATION", 
    "MIGRATION", "DISPLACED", "FOOD", "AFFECT", "URBAN", "RURAL", "TOURISM", 
    "ELDERLY", "DISABILITY", "HOLIDAY", "GENDER", "YOUTH", "LGBT", "INDIGENOUS", 
    "RICE", "BREAD", "MEAT", "FAMINE", "SHORTAGE", "RAPE", "SEXUAL", "OBESITY",
    "DIABETES", "ALCOHOL", "VULNERABLE", "INEQUALITY", "FREESPEECH", "LITERACY",
    "CASH_TRANSFERS", "IMMUNIZATIONS", "ORPHANS", "HOUSING", 
    "SLAVERY", "BULLYING", "WELLBEING", "DEMOGRAPHIC",
    "AGING", "LIFE_EXPECTANCY", "HYPERTENSION", "INFLUENZA", "DEWORMING", 
    "MIDWIVES", "BREASTFEEDING", "EMERGENCYROOM", "SLUMS", "SAFETY_NETS",
    "FAMILY_PLANNING", "WORKER", "PLACEMENT", "OPPORTUNITIES", "BENEFITS",
    "RESETTLEMENT", "NURSING_HOMES", "CARE", "SICK", "CONSUMER_PROTECTION",
    "GERIATRICS", "CONTRACEPTIVES", "STUNTING", "MALARIA", "TUBERCULOSIS",
    "EBOLA", "CHOLESTEROL", "MIDDLE_CLASS", "GENTRIFICATION", "ETHNIC",
    "MINORITIES", "MATERNITY", "MICRONUTRIENTS", "PREMATURE_DEATH", "SEDENTARY",
    "TOBACCO", "STD", "STI", "EARLY_MARRIAGE", "FUNGUS", "STREET_CHILDREN",
    "INCLUSIVE", "POPULATION", "ALLOWANCE", "AFFIRMATIVE_ACTION", "VOICE_AND_AGENCY",
    "NATURALIZATION", "PHARMACEUTICAL", "HIV", "AIDS", "SURVIVAL_SEX", 
    "PATERNITY", "FERTILITY", "MORTALITY", "ACCESS_TO_SERVICES", "LOW_INCOME",
    "REUNIFICATION", "OBSTETRIC", "TRACER_STUDY", "INDIGINOUS", "MEDICINES",
    "INJURIES", "BENEFIT_ADMINISTRATION"
  ],
  "education_culture": [
    "EDUCATION", "RELIGION", "CULTURE", "SCHOOL", "UNIVERSITY", "ARTS", "HISTORY",
    "SCHOLARSHIPS", "GRADUATION", "TEXTBOOKS", "LIFELONG_LEARNING", "ANTISEMITISM",
    "TEACHER", "LANGUAGE", "ISLAMIZATION", "PEDAGOGICAL", "CULTURAL",
    "ENROLLMENT", "TRACER", "LEARNING", "QUALIFICATION", "ACCREDITATION", "RADIO_INSTRUCTION"
  ],
  "tech_infrastructure": [
    "INFRASTRUCTURE", "TRANSPORT", "ICT", "DIGITAL", "SCIENCE", "CYBER", 
    "CONSTRUCTION", "MARITIME", "PORTS", "AIRPORTS", "ROADS", "HIGHWAYS", 
    "TRAFFIC", "NETWORK", "SERVER", "DATABASE", "AUTHENTICATION", "SATELLITE",
    "AUTOMATION", "RAILWAYS", "DELAY", "MOVEMENT", "CONGESTION", "SWITCHES",
    "INNOVATION", "TECHNOLOGY", "CONNECTIVITY", "STORAGE", "SEWERAGE", "DAMS", 
    "RESERVOIRS", "POWER_OUTAGE", "TELECOMMUNICATIONS", "INTEROPERABILITY",
    "INCIDENT", "ENCRYPTION", "OPEN_SOURCE", "OPERATING_SYSTEMS", "VIRTUALREALITY",
    "MOBILE_APPLICATIONS", "AVIATION", "ROAD_SAFETY", "CITY_STRATEGIES",
    "LAND_TENURE", "CADASTRE", "SMART_CITIES", "MASS_TRANSIT",
    "SUPERCOMPUTING", "BIG_DATA", "CLOUD_COMPUTING", "BIGDATA", "PRIVACY",
    "SMART_GRIDS", "INTERACTION", "GAMIFICATION", "E_MONEY", "COCREATION",
    "BROADBAND", "DATA", "KNOWLEDGE", "DISTRIBUTION", "TRANSMISSION", "OUTAGE",
    "ERP_", "PEOPLESOFT", "ARCHITECT", "CATALOGUES", "CROWDSOURCING",
    "EXPERT_SOURCING", "MOBILE_DEVICE", "WEB_FARM", "3D_PRINTING", "API_",
    "APPLICATION_PROGRAMMING", "ACCESS_AND_CONNECTIVITY", "SATELLITES",
    "CITY_SYSTEMS", "INNOVATIVE_CITIES", "IDENTITY", "DAM_SAFETY", "BENCHMARKING",
    "COMMUNITY_MAPPING"
  ],
  "media_communication": [
    "MEDIA", "BROADCAST", "JOURNALISM", "CENSORSHIP", "INTERNET", "INFORMATION", 
    "RUMOR", "HOAX", "POSTAL_SERVICES", "COURIER", "PHONE_OUTAGE"
  ],
  "taxonomy_nature_misc": [
    "TAX_FNCACT", "TAX_MILITARY_TITLE", "TAX_ETHNICITY", "TAX_WORLDLANGUAGES", 
    "TAX_POLITICAL_PARTY", "TAX_TERROR_GROUP", "TAX_RELIGION", "TAX_WORLDMAMMALS",
    "TAX_WORLDFISH", "TAX_WORLDINSECTS", "TAX_WORLDARACHNIDS", "FISH", "INSECT", 
    "BEE", "CRUSTACEANS", "REPTILES", "BIRDS", "DUCK", "LOCUSTS", "CENTIPEDE",
    "MYRIAPODA", "SUPPLEMENTARY"
  ]
}

# --- 3. FUNZIONI HELPER ---

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        print(f"❌ Errore connessione DB: {e}")
        sys.exit(1)

def process_themes(raw_themes_string):
    """
    Prende la stringa grezza 'CODE1;CODE2', la pulisce e la classifica.
    Restituisce: (stringa_temi_puliti, stringa_categorie)
    """
    if not raw_themes_string:
        return None, None
        
    raw_list = raw_themes_string.split(';')
    cleaned_list = []
    categories_set = set()
    
    for original_code in raw_list:
        if not original_code or original_code == '<NA>': continue
        
        # 1. Pulizia Etichetta (Regex)
        s = re.sub(r'^(WB_\d+_|UNGP_|USPEC_|CRISISLEX_[A-Z0-9]*_|EPU_|TAX_FNCACT_|TAX_AIDGROUPS_|TAX_SPECIAL_ISSUES_|TAX_SPECIAL_|TAX_|SOC_|GEN_|SLFID_|INFO_|ETH_|CRM_|ACT_|REL_|MIL_|MED_|AVIATION_|TECH_|RAIL_|EMERG_)', '', original_code)
        s = re.sub(r'^[0-9A-Z]+_\d+_', '', s)
        s = s.replace('_', ' ')
        cleaned_label = s.strip().lower()
        cleaned_list.append(cleaned_label)
        
        # 2. Categorizzazione
        found_cat = False
        for category, keywords in THEME_MAPPING_RULES.items():
            for k in keywords:
                if k in original_code:
                    categories_set.add(category.lower())
                    found_cat = True
                    break
            if found_cat: break
        if not found_cat:
            categories_set.add("unclassified")

    # Unisci i risultati
    final_themes = '; '.join(cleaned_list)
    final_categories = '; '.join(sorted(list(categories_set)))
    
    return final_themes, final_categories

# --- 4. CORE FUNCTIONS ---

def fix_cameo_events(conn):
    """FASE 1: Decodifica CAMEO nella tabella EVENT."""
    print("\n--- FASE 1: Decodifica Eventi (CAMEO) ---")
    try:
        cursor = conn.cursor()
        print("🛠  Modifica colonna EventRootCode in TEXT...")
        cursor.execute("ALTER TABLE EVENT ALTER COLUMN EventRootCode TYPE TEXT;")
        conn.commit()
        
        print("🔄 Aggiornamento descrizioni eventi...")
        when_clauses = []
        for code, desc in CAMEO_EVENT_MAPPING.items():
            safe_desc = desc.replace("'", "''").lower()
            when_clauses.append(f"WHEN EventRootCode = '{code}' THEN '{safe_desc}'")
        
        # Applica solo ai codici numerici che combaciano esattamente
        query = f"""
            UPDATE EVENT
            SET EventRootCode = CASE 
                {' '.join(when_clauses)}
                ELSE LOWER(EventRootCode)
            END
            WHERE EventRootCode ~ '^[0-9]+$';
        """
        cursor.execute(query)
        conn.commit()
        print(f"✅ Eventi aggiornati: {cursor.rowcount}")
    except Exception as e:
        print(f"❌ Errore Fase 1: {e}")
        conn.rollback()

def denormalize_themes(conn):
    """FASE 2: Aggiunge colonne pulite ad ARTICLE e le popola."""
    print("\n--- FASE 2: Appiattimento Temi in ARTICLE ---")
    cursor = conn.cursor()
    
    # 1. Aggiungi le nuove colonne
    print("🛠  Aggiunta colonne 'Themes_Human' e 'Themes_Category'...")
    cursor.execute("ALTER TABLE ARTICLE ADD COLUMN IF NOT EXISTS Themes_Human TEXT;")
    cursor.execute("ALTER TABLE ARTICLE ADD COLUMN IF NOT EXISTS Themes_Category TEXT;")
    conn.commit()
    
    # Creazione Indici per RAG veloce
    print("⚡ Creazione indici sulle nuove colonne...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_article_themes_human ON ARTICLE USING gin(Themes_Human gin_trgm_ops);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_article_themes_cat ON ARTICLE(Themes_Category);")
    conn.commit()

    # 2. Loop di Aggiornamento (Batch Processing)
    print("🔄 Inizio elaborazione righe (Batch loop)...")
    
    total_processed = 0
    
    while True:
        # Prendi un blocco di righe che non sono ancora state processate
        cursor.execute(f"""
            SELECT DocIdentifier_Normalized, EnhancedThemes 
            FROM ARTICLE 
            WHERE Themes_Human IS NULL AND EnhancedThemes IS NOT NULL
            LIMIT {BATCH_SIZE};
        """)
        rows = cursor.fetchall()
        
        if not rows:
            break # Finito!
            
        updates = []
        for doc_id, raw_themes in rows:
            clean, cats = process_themes(raw_themes)
            updates.append((clean, cats, doc_id))
            
        # Esegui update del batch
        # Usiamo execute_values o un loop di update ottimizzato? 
        # Per semplicità e sicurezza, usiamo executemany
        query = "UPDATE ARTICLE SET Themes_Human = %s, Themes_Category = %s WHERE DocIdentifier_Normalized = %s"
        cursor.executemany(query, updates)
        conn.commit()
        
        total_processed += len(rows)
        sys.stdout.write(f"\r   ...processati {total_processed} articoli.")
        sys.stdout.flush()

    print(f"\n✅ FASE 2 Completata! Totale: {total_processed}")

# --- MAIN ---

def main():
    print("🚀 Inizio Script Completo (Fix + Flattening)")
    conn = get_db_connection()
    
    fix_cameo_events(conn)
    denormalize_themes(conn)
    
    conn.close()
    print("\n🎉 DATABASE AGGIORNATO! Ora puoi fare query semplici.")
    print("Esempio: SELECT * FROM ARTICLE WHERE Themes_Category LIKE '%economy%'")

if __name__ == "__main__":
    main()