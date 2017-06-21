from datetime import datetime
from datetime import time
from datetime import timedelta
import re
from pandas import Series, DataFrame
import pandas as pd

import app.models as models


ansmap = {
    "Addictive": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Affectionate / Loving": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Antibacterial/Disinfecting": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Apathetic, Dull, Sluggish": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Artificial/Chemical": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Beautiful": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Buy perfume": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Calm, Relaxed, Tranquil": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Casual": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Cheap": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Classic": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Clean": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Closing": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Clothes in the closet": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Confident": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Contemporary": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Distinctive": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Distinctive / Unique": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Doing the laundry": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Easy to wear": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Elegant": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Energizing": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Familiar": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Feminine": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "For whole family": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Fresh": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Fresh Air / Breezy": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Glamorous": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Happy": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Happy, Pleased, Delighted": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Harsh": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Has character": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Healthy": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Heavy": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "High quality": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Indulgent  ": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Innocent": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Ironing": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Irritated, Frustrated, Agitated": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Laundry bars": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Light/mild": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Liquid Detergent": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Long lasting": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Luxurious": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Luxurious/rich": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Makes me feel good": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Masculine": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Medicinal /Therapeutic": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Modern": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Natural": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "New / never smelled before": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "None": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "None": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "None": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Nostalgic / memorable": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Nourishing / caring": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Old-fashioned": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "On bed": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Open": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Outdoor": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Powder Detergent": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Powerful": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Premium": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Pure": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Rejuvenating": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Relaxing": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Removing clothes line": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Romantic": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Sad, Gloomy, Depressed": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Scent boosters": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Seductive": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Sensual": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Sensuous, Romantic, Sexy": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Softener ": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Sophisticated": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Sporty/Athletic": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Stimulated, Lively, Excited ": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Subtle": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Tense, Anxious, Stressed": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Traditional": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Trendy": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Unique": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Wearing at the end day": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Wearing first time": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Wet clothes drying line": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Wet laundry ": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "When using towels": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),
    "Youthful": ({"Yes" : "Yes", "No": "No"}, {"Yes": ["1", "Yes"], "No": ["0", "No"]}),

    "Buy perfume": ({1 : "Yes", 0: "No"}, {"1": [1, "Yes"], "0": [0, "No"]}),
    "Fragrance perception": ({}, {"Clean" : ["2", "Clean"], "Fresh": ["3", "Fresh"], "Long lasting" : ["1", "Long lasting"]}),
    "Frequency": ({}, {"occasionally":["Occasionally"], "regularly":["Regularly"]} ),
    "Gender frag": ({}, {"Female":["F"], "Male":["M"],"Unisex":["U"],"Other":["00OTHER"]} ),
    "Gender": ({}, {"Male" : ["Male", "Man"], "Female": ["Femal", "Woman"]}),
    "Liking": ({7 : "7 Like very much", 6 : "6 Like moderately", 5 : "5 Like a little", 4:  "4 Neither like / dislike", 3 : "3 Dislike it a little", 2 : "2 Disike moderately", 1 : "1 Disike very much"},
               {"7 Like very much" : ["7"], "6 Like moderately": ["6"], "5 Like a little" : ["5"], "4 Neither like / dislike": ["4"], "3 Dislike it a little" : ["3"], "2 Disike moderately": ["2"], "1 Disike very much": ["1"]}),
    "Occasions": ({}, {"everyday": ["everyday"], "everyday and special": ["good for both everyday and special occasions"], "special": ["special occasions"]}),
    "How discover": ({}, {"myself": ["I chose it myself"], "present": ["It was a present"]}),
    "Season": ({}, {"summer": ["more for summer time"], "winter": ["more for winter time"] , "both": ["good for both summer and winter"]}),
    "Wear when perfume": ({}, {"weeks" : ["A few weeks"], "months": ["A few months"], "year" : ["More than a year"], "3 years" : ["More than 3 years"], "10 years": ["More than 10 years"]}),

    }


# Direct mapping of a column to a ElasticSearch field
# In case multiple columns are mapped to the same field, the one column with a value is loaded (variant for Fresh&Clean)
col2fld = {
    'resp_id'   : ["RESPID - RESPONDENT ID", "Resp No/ID"],
    'survey'    : ["Year"],
    'country'   : ["COUNTRY - COUNTRY", "Country"],
    'cluster'   : ["Cluster", "Choice model Cluster"],
    'ethnics'   : ["Ethnies"],
    'city'      : ["City", "Test City"],
    'regions'   : ["Regions"],
    'education' : ["Education"],
    'income'    : ["Income"],
    'blindcode' : ["Code", "Product Code", "Sample"],
    'brand'     : ["Brand", "Global Brand BUMO"],
    'variant'   : ["variant", "brand used most often liquid detergent + variant", "brand used most often powder detergent + variant"],
    'olfactive' : ["FF", "olfactive Family"],
    ## Fresh & Clean
    "method"    : ["Wash Method"],
    "freshness" : ["h9_Freshness"],
    "cleanliness" : ["h9_Cleanliness"],
    "lastingness" : ["h9_Long lastingness"],
    "intensity" : ["j_JAR Strength"],
    }

ans2fld = {
    'age'           : ["Age group"],
    "product_form"  : ["Product Form"],
    'gender'        : ["Gender"],
    "liking"        : ["Liking"],
    "perception"    : ["Fragrance perception"],
    }

aggr2ans = {
    "Liking"        : ["liking.keyword"],
    }

col2ans = {
    "No children home"  : ["No children at home"],
    "Children aged 0-6" : ["Child(ren) aged 0-6 years"],
    "Children aged 7-16": ["Child(ren) aged 7-16 years"],
    "Children aged > 16": ["Child(ren) aged more than 16 years"],
    "Casual": ["Casual"],
    "Distinctive": ["Distinctive"],
    "Sensual": ["Sensual"],
    "Has character": ["Has character"],
    "Easytowear": ["Easy to wear"],
    "Subtle": ["Subtle"],
    "Powerful": ["Powerful"],
    "Traditional": ["Traditional"],
    "Unique": ["Unique"],
    "Luxurious": ["Luxurious"],
    "Contemporary": ["Contemporary"],
    "Romantic": ["Romantic"],
    "Seductive": ["Seductive"],
    "Happy": ["Happy"],
    "Energizing": ["Energizing"],
    "Long lasting": ["Long lasting scent"],
    "Relaxing": ["Relaxing"],
    "Wear when perfume": ["when do you wear this perfume?"],
    "Buy perfume": ["Do you think you will buy this perfume again?"],
    "Occasions": ["Occasions"],
    "Season": ["Season"],
    "How discover": ["How did you discover this perfume?"],
    "Frequency": ["Frequency"],
    "Gender frag" : ["Gender frag"],
    "Wear nr perfumes" : ["How many different perfumes do you currently wear?"],
    ## Fresh & Clean
    "survey": ["Year"],
    "country": ["Country"],
    "city": ["Test City"],
    "Gender": ["Woman/Man", "Gender"],
    "Age group": ["Age cat", "Age group"],
    "method": ["Wash Method"],
    "Product Form": ["Detergent format"],
    "brand": ["Global Brand BUMO"],
    "variant": ["brand used most often liquid detergent + variant"],
    "variant": ["brand used most often powder detergent + variant"],
    "cluster": ["Choice model Cluster"],
    "blindcode": ["Product Code"],
    "olfactive": ["olfactive Family"],
    "freshness": ["h9_Freshness"],
    "cleanliness": ["h9_Cleanliness"],
    "lastingness": ["h9_Long lastingness"],
    "intensity": ["j_JAR Strength"],
    "Liking": ["h7_overall_liking fragrance"],
    "Liquid Detergent": ["Is this a smell you would like to have in a_Liquid Detergent"],
    "Powder Detergent": ["Is this a smell you would like to have in a_Powder Detergent"],
    "Laundry bars": ["Is this a smell you would like to have in a_Laundry bars (ASIA LATAM AFRICA)/Unit dose (EAME)"],
    "Softener ": ["Is this a smell you would like to have in a_Softener"],
    "Scent boosters": ["Is this a smell you would like to have in a_Scent boosters"],
    "None": ["Is this a smell you would like to have in a_None"],
    "Open": ["When open the pack"],
    "Closing": ["While dosing"],
    "Doing the laundry": ["While doing the laundry"],
    "Wet laundry ": ["On wet laundry coming out of the machine"],
    "Wet clothes drying line": ["When hanging wet clothes on the line/when drying"],
    "Removing clothes line": ["When removing clothes from the line/dryer"],
    "Clothes in the closet": ["My clothes in the closet"],
    "Ironing": ["While ironing"],
    "Wearing first time": ["When wearing clothes for the first time"],
    "Wearing at the end day": ["At the end of the day wearing my clothes"],
    "When using towels": ["When using towels"],
    "On bed": ["On bed using bed sheets"],
    "None": ["None"],
    "Pair 1": ["Pair 1"],
    "Pair 2": ["Pair 2"],
    "Pair 3": ["Pair 3"],
    "Pair 4": ["Pair 4"],
    "Pair 5": ["Pair 5"],
    "Pair 6": ["Pair 6"],
    "Artificial/Chemical": ["a1_Artificial/Chemical"],
    "Heavy": ["a1_Heavy"],
    "Innocent": ["a1_Innocent"],
    "Luxurious/rich": ["a1_Luxurious/rich"],
    "Makes me feel good": ["a1_Makes me feel good"],
    "Masculine": ["a1_Masculine"],
    "Natural": ["a1_Natural", "Natural"],
    "Old-fashioned": ["a1_Old-fashioned"],
    "Addictive": ["a1_Addictive"],
    "Affectionate / Loving": ["a1_Affectionate / Loving"],
    "Antibacterial/Disinfecting": ["a1_Antibacterial/Disinfecting"],
    "Fresh Air / Breezy": ["a1_Fresh Air / Breezy"],
    "Classic": ["a1_Classic", "Classic"],
    "Confident": ["a1_Confident"],
    "Healthy": ["a1_Healthy"],
    "Rejuvenating": ["a1_Rejuvenating"],
    "Clean": ["a1_Clean"],
    "Light/mild": ["a1_Light/mild"],
    "Sophisticated": ["a1_Sophisticated", "Sophisticated"],
    "Sporty/Athletic": ["a1_Sporty/Athletic"],
    "High quality": ["a1_High quality"],
    "For whole family": ["a1_For whole family"],
    "Glamorous": ["a1_Glamorous"],
    "Medicinal /Therapeutic": ["a1_Medicinal /Therapeutic"],
    "Modern": ["a1_Modern", "Modern"],
    "New / never smelled before": ["a1_New / never smelled before"],
    "Nostalgic / memorable": ["a1_Nostalgic / memorable"],
    "Distinctive / Unique": ["a1_Distinctive / Unique"],
    "Beautiful": ["a1_Beautiful"],
    "Cheap": ["a1_Cheap"],
    "Indulgent  ": ["a1_Indulgent"],
    "Elegant": ["a1_Elegant", "Elegant"],
    "Feminine": ["a1_Feminine", "Very feminine/Very masculine"],
    "Fresh": ["a1_Fresh", "Fresh"],
    "Harsh": ["a1_Harsh"],
    "Nourishing / caring": ["a1_Nourishing / caring"],
    "Outdoor": ["a1_Outdoor"],
    "Premium": ["a1_Premium"],
    "Pure": ["a1_Pure"],
    "Trendy": ["a1_Trendy", "Trendy"],
    "Youthful": ["a1_Youthful"],
    "Familiar": ["a1_Familiar"],
    "Long lasting fragrance": ["a1_Long lasting fragrance"],
    "Strong scents": ["a1_Strong scents"],
    "Extra Soft / caring ": ["a1_Extra Soft / caring "],
    "Super clean efficacy": ["a1_Super clean efficacy"],
    "Extra Stain remover effect": ["a1_Extra Stain remover effect"],
    "Extra Whitening": ["a1_Extra Whitening"],
    "Fresher scent": ["a1_Fresher scent"],
    "Extra Softening effect": ["a1_Extra Softening effect"],
    "Malodor elimination": ["a1_Malodor elimination"],
    "Fragrance booster": ["a1_Fragrance booster"],
    "Color clothes protection": ["a1_Color clothes protection"],
    "Environment protection": ["a1_Environment protection"],
    "Skin protection": ["a1_Skin protection"],
    "Protect/keep shape on clothes ": ["a1_Protect/keep shape on clothes"],
    "Easy ironing": ["a1_Easy ironing"],
    "Fast ironing": ["a1_Fast ironing"],
    "Disinfectant /antibacterial effect on clothes": ["a1_Disinfectant /antibacterial effect on clothes"],
    "Anti-wrinkle": ["a1_Anti-wrinkle"],
    "Anti-shrinking": ["a1_Anti-shrinking"],
    "Extra Brightening clothes": ["a1_Extra Brightening clothes"],
    "Anti-mold fungus": ["a1_Anti-mold fungus"],
    "Fast dry clothes": ["a1_Fast dry clothes"],
    "Gentle on clothes": ["a1_Gentle on clothes"],
    "Easy rinse ": ["a1_Easy rinse"],
    "Kill bacteria": ["a1_Kill bacteria"],
    "Long lasting freshness": ["a1_Long lasting freshness"],
    "Other": ["a1_Other"],
    "None": ["a1_None"],
    "Happy, Pleased, Delighted": ["a1_mood_Happy, Pleased, Delighted"],
    "Calm, Relaxed, Tranquil": ["a1_mood_Calm, Relaxed, Tranquil"],
    "Sensuous, Romantic, Sexy": ["a1_mood_Sensuous, Romantic, Sexy"],
    "Stimulated, Lively, Excited ": ["a1_mood_Stimulated, Lively, Excited "],
    "Apathetic, Dull, Sluggish": ["a1_mood_Apathetic, Dull, Sluggish"],
    "Sad, Gloomy, Depressed": ["a1_mood_Sad, Gloomy, Depressed"],
    "Irritated, Frustrated, Agitated": ["a1_mood_Irritated, Frustrated, Agitated"],
    "Tense, Anxious, Stressed": ["a1_mood_Tense, Anxious, Stressed"],
    "Fragrance perception": ["would you say this fragrance is"],
    "Fresh Sensorial  (Premium)": ["Fresh Sensorial  (Premium)"],
    "Fresh Confident  ( Makes me feel good)": ["Fresh Confident  ( Makes me feel good)"],
    "Fresh Sensorial  ( Feminine)": ["Fresh Sensorial  ( Feminine)"],
    "Fresh Essential (Natural & protection )": ["Fresh Essential (Natural & protection )"],
    "Fresh Revitalizing  (rejuvenating & Outdoors)": ["Fresh Revitalizing  (rejuvenating & Outdoors)"],
    "Clean  & Premium ": ["Clean  & Premium "],
    "Clean & Makes me feel good": ["Clean & Makes me feel good"],
    "Clean & Feminine": ["Clean & Feminine"],
    "Long Lasting & Premium": ["Long Lasting & Premium"],
    "Long Lasting & Makes me feel good": ["Long Lasting & Makes me feel good"],
    "Long Lasting & Harsh": ["Long Lasting & Harsh"],
    "Long Lasting & Feminine": ["Long Lasting & Feminine"],
    }

ans2qst = {
    "smell" : ["Easy to wear","Subtle","Powerful","Trendy","Traditional","Unique","Luxurious","Contemporary","Romantic","Happy"],
    "fragrattr" : ["Feminine","Seductive","Energizing","Long lasting", "Relaxing", "Buy perfume"],
    "question"  : ["Wear when perfume", "Frequency", "Season", "Gender frag", "Wear nr perfumes" ],
    'children'  : ["No children home", "Children aged 0-6", "Children aged 7-16", "Children aged > 16"],
    ## Fresh & Clean

    "concept" : [
	            "Anti-mold fungus",
	            "Anti-shrinking",
	            "Anti-wrinkle",
	            "Color clothes protection",
	            "Disinfectant /antibacterial effect on clothes",
	            "Easy ironing",
	            "Easy rinse ",
	            "Environment protection",
	            "Extra Brightening clothes",
	            "Extra Soft / caring ",
	            "Extra Softening effect",
	            "Extra Stain remover effect",
	            "Extra Whitening",
	            "Fast dry clothes",
	            "Fast ironing",
	            "Fragrance booster",
	            "Fresher scent",
	            "Gentle on clothes",
	            "Kill bacteria",
	            "Long lasting fragrance",
	            "Long lasting freshness",
	            "Malodor elimination",
	            "None",
	            "Other",
	            "Protect/keep shape on clothes ",
	            "Skin protection",
	            "Strong scents",
	            "Super clean efficacy",
                ],

    "emotion"   : [
	                "Addictive",
	                "Affectionate / Loving",
	                "Airy",
	                "Antibacterial/Disinfecting",
	                "Artificial/Cheap",
	                "Artificial/Chemical",
	                "Beautiful",
	                "Casual",
	                "Cheap",
	                "Classic",
	                "Clean",
	                "Comforting/Relaxing",
	                "Confident",
                    "Distinctive",
	                "Distinctive / Unique",
	                "Elegant",
	                "Elegant/ Luxurious",
	                "Expensive/Sophisticated",
	                "Familiar",
	                "Feminine",
	                "For Both Men And Women",
	                "For Daytime",
	                "For Evening/Nighttime",
	                "For whole family",
	                "Fresh",
	                "Fresh Air / Breezy",
	                "Friendly/ Outgoing",
	                "Glamorous",
	                "Harsh",
	                "Harsh/ Chemical",
	                "Has Character",
	                "Healthy",
	                "Heavy",
	                "High quality",
	                "Indulgent  ",
	                "Innocent",
	                "Invigorating",
	                "Light/Delicate",
	                "Light/mild",
	                "Luxurious/rich",
	                "Makes me feel good",
	                "Makes Me Feel Good",
	                "Masculine",
	                "Medicinal /Therapeutic",
	                "Mild",
	                "Modern",
	                "Modern/Contemporary",
	                "Natural",
	                "New / never smelled before",
	                "Nostalgic / memorable",
	                "Nourishing / caring",
	                "Old-fashioned",
	                "Outdoor",
	                "Premium",
	                "Pure",
	                "Refreshing",
	                "Rejuvenating",
	                "Romantic",
	                "Sensual",
	                "Sexy",
	                "Sophisticated",
	                "Sporty/Athletic",
	                "Trendy",
	                "Trendy",
	                "Unforgettable",
	                "Warm",
	                "Well Rounded",
	                "Youthful",
                    ],
    "mood"      : [
                    "Apathetic, Dull, Sluggish",
                    "Calm, Relaxed, Tranquil",
                    "Happy, Pleased, Delighted",
                    "Irritated, Frustrated, Agitated",
                    "Sad, Gloomy, Depressed",
                    "Sensuous, Romantic, Sexy",
                    "Stimulated, Lively, Excited ",
                    "Tense, Anxious, Stressed",
                    ],
    "suitable_product" : [
	                "Liquid Detergent",
	                "Powder Detergent",
	                "Laundry bars",
	                "Softener ",
	                "Scent boosters",
	                "None",
                    ],
    "suitable_stage" : [
	                "Open",
	                "Closing",
	                "Doing the laundry",
	                "Wet laundry ",
	                "Wet clothes drying line",
	                "Removing clothes line",
	                "Clothes in the closet",
	                "Ironing",
	                "Wearing first time",
	                "Wearing at the end day",
	                "When using towels",
	                "On bed",
	                "None",
                    ],

    }

qst2fld = {
    "concept"   : ["concept"],
    "children"  : ["children"],
    "emotion"   : ["emotion"],
    "fragrattr" : ["fragrattr"],
    "mood"      : ["mood"],
    "smell"     : ["smell"],
    "question"  : ["question"],
    "suitable_product"  : ["suitable_product"],
    "suitable_stage"  : ["suitable_stage"],
    }


def answer_value_to_string(answer_value):
    if type(answer_value) == int:
        answer_value = "{0:d}".format(answer_value)
    elif type(answer_value) == float:
        answer_value = "{0:.2f}".format(answer_value)
    return answer_value


def answer_value_encode(answer, answer_value):
    global ansmap

    answer_code = answer_value
    if answer in ansmap:
        # ansmap[0] is the decoder, ansmap[1] is the encoder
        for value, map in ansmap[answer][1].items():
            if answer_value in map:
                answer_code = value
                break
    return answer_code
 
def answer_value_decode(answer, answer_code):
    global ansmap

    answer_value = answer_code
    if answer in ansmap:
        # ansmap[0] is the decoder, ansmap[1] is the encoder
        for answer_value, map in ansmap[answer][0].items():
            if answer_code == map:
                return answer_value
            elif type(answer_code) == str:
                if answer_code.isdigit():
                    answer_digit_code = int(float(answer_code))
                    if answer_digit_code == map:
                        return answer_value
    return answer_code

       
def seekerview_answer_value_decode(seererview, answer, answer_code):
    return answer_value_decode(answer, answer_code)


def col_map_field(column):
    global col2fld

    for field, columns in col2fld.items():
        if column.strip() in columns:
            return field
    return None

def ans_map_field(answer):
    global ans2fld

    for field, answers in ans2fld.items():
        if answer.strip() in answers:
            return field
    return None

def aggr_map_ans(aggr):
    global aggr2ans

    for answer, aggrs in aggr2ans.items():
        if aggr in aggrs:
            return answer
    return None

def qst_map_field(question):
    global qst2fld

    for field, questions in qst2fld.items():
        if question in questions:
            return field
    return None

def ans_map_question(answer):
    global ans2qst

    for question, answers in ans2qst.items():
        if answer in answers:
            return question
    return None

def col_map_answer(column):
    global col2ans

    for answer, columns in col2ans.items():
        if column.strip() in columns:
            return answer
    return None

def field_map_dashboard(field):
    return None



def map_column(column):
    # returns ES fieldname, question and nested ES fieldname (=answer)
    global ans2qst
    global col2ansvalue

    # Check on a direct mapping between column and field
    field = col_map_field(column)
    if field:
        return field, None, None
    # Check on a mapping to an answer
    answer = col_map_answer(column)
    if answer:
        # Check on a direct mapping between answer and field
        field = ans_map_field(answer)
        if field:
            return field, None, answer
        # Check wheter answer belongs to a question
        question = ans_map_question(answer)
        if question:
            # Check on a mapping between question and field
            field = qst_map_field(question)
            return field, question, answer
    return None, None, None


def map_columns(columns):
    field_map = {}
    col_map = {}
    for column in columns:
        field, question, answer = map_column(column)
        dbname = field_map_dashboard(field)
        col_map[column] = (field, question, answer, dbname)
        if field != None:
            if field not in field_map.keys():
                field_map[field] = [(question, answer, column)]
            else:
                field_map[field].append((question, answer, column))
    return field_map, col_map



