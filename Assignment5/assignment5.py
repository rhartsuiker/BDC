"""
Dit script krijgt als input een InterPROscan output file;
je kunt tests runnen op de example data in de data/datasets/EBI/interpro/refseqscan directories
op alle BIN netwerk computers. Je moet voor deze opdracht de PySpark Dataframe interface gebruiken
om de data in te lezen en manipuleren. De files in bovenstaande directory zijn enkele gigabytes
groot en bevatten steeds ~200,000 protein annotations. De opdracht is om onze network controller
(spark://spark.bin.bioinf.nl) Je moet de PySpark dataframe functionaliteit gebruiken om de volgende
vragen te beantwoorden:

    1. Hoeveel protein annotations zijn er in deze dataset, m.a.w. hoeveel uniek InterPRO nummers
       zijn er?
    2. Hoeveel InterPRO annotaties heeft een eiwit gemiddeld?
    3. Van de GO Terms die ook geannoteerd zijn, wat is de meest voorkomende?
    4. Wat is de grootte (in Amino Acids) van een InterPRO feature in deze dataset?
    5. Wat is de top 10 meest voorkomende InterPRO annotaties?
    6. Als je selecteert op InterPRO annotaties die >90% van het eiwit omspannen, wat is dan de
       top 10 van meestgevonden InterPRO annotaties?
    7. Voor die annotaties waar ook text bij staat; in die texts, wat zijn de 10 meest voorkomende
       annotaties? En wat zijn de top 10 minst voorkomende?
    8. Als je de antwoorden van vraag 6 en 7 combineert, voor die grootste (>90% lengte van het eiwit)
       features, wat zijn daar de top 10 voorkomende woorden voor?
    9. Wat is de coefficient (R^2) tusen de grootte van het eiwit zelf, en het aantal InterPRO
       annotaties dat er gevonden wordt?

Je output moet een CSV file met 3 kolommen zijn:

    Eerste kolom: vraag nummer
    Tweede kolom: antwoord op die vraag
    Derde kolom: output van de scheduler zijn plan, verkregen met de explain() methode
    (zie het college)
"""


"""
   1. Protein accession (e.g. P51587)
   2. Sequence MD5 digest (e.g. 14086411a2cdf1c4cba63020e1622579)
   3. Sequence length (e.g. 3418)
   4. Analysis (e.g. Pfam / PRINTS / Gene3D)
   5. Signature accession (e.g. PF09103 / G3DSA:2.40.50.140)
   6. Signature description (e.g. BRCA2 repeat profile)
   7. Start location
   8. Stop location
   9. Score - is the e-value (or score) of the match reported by member database method (e.g. 3.1E-52)
   10. Status - is the status of the match (T: true)
   11. Date - is the date of the run
   12. InterPro annotations - accession (e.g. IPR002093)
   13. InterPro annotations - description (e.g. BRCA2 repeat)
   14. (GO annotations (e.g. GO:0005515) - optional column; only displayed if goterms option is switched on)
   15. (Pathways annotations (e.g. REACT_71) - optional column; only displayed if pathways option is switched on)
"""


interpro_schema = 'prot_access string, seq_md5 string, seq_len int, analysis string, sign_access string, sign_descr string, ' + \
   'start int, stop int, score string, status string, date string, inter_anno_access string, inter_anno_descr string, ' + \
   'go_anno string, path_anno string'


import sys
import pyspark.sql.functions as psf

from pyspark.sql import SparkSession


def create_unique_annotation_row(df):
   return [1, df.select('inter_anno_access').distinct().count(), df.explain()]


def create_avg_annotation_per_prot_row(df):
   return [2, df.count() / df.select('prot_access').distinct().count(), df.explain()]


def create_most_common_go_annotation_row(df):
   return [3, df.where((df.go_anno != 'None') & (df.go_anno != '-')).groupby('go_anno').count().orderBy(psf.desc('count')).take(1)[0].__getitem__('go_anno'), df.explain()]


def main(args):
   filename = args[1]
   spark = SparkSession.builder.getOrCreate()

   df = spark.read.csv(filename, sep=r'\t', header=False, schema=interpro_schema)

   write_rows = []

   write_rows.append(create_unique_annotation_row(df))
   write_rows.append(create_avg_annotation_per_prot_row(df))
   write_rows.append(create_most_common_go_annotation_row(df))

################################TEST##########################################################





################################TEST##########################################################


   return


if __name__ == "__main__":
    sys.exit(main(sys.argv))
