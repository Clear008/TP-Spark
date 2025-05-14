package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App1 {
    public static void main(String[] args) {
        // Configuration de Spark
        SparkConf conf = new SparkConf()
                .setAppName("Total Ventes Par Ville")
                .setMaster("local[*]"); // Mode local pour les tests

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Définition du chemin du fichier d'entrée
        String inputPath = "ventes.txt";

        // Lecture du fichier
        JavaRDD<String> ventesRDD = sc.textFile(inputPath);

        // Transformation des données
        // Structure: date ville produit prix
        JavaPairRDD<String, Double> ventesParVille = ventesRDD
                .mapToPair(ligne -> {
                    String[] champs = ligne.split(" ");
                    // Extraction de la ville (champ 1) et du prix (champ 3)
                    String ville = champs[1];
                    Double prix = (Double) Double.parseDouble(champs[3]);
                    return new Tuple2<>(ville, prix);
                })
                .reduceByKey((prix1, prix2) -> (Double) (prix1 + prix2)); // Somme des prix par ville

        // Affichage des résultats
        System.out.println("Total des ventes par ville:");
        ventesParVille.collect().forEach(resultat -> {
            System.out.println(resultat._1() + ": " + resultat._2());
        });

        // Sauvegarde des résultats si nécessaire
        // ventesParVille.saveAsTextFile("resultats_ventes_par_ville");

        // Arrêt du contexte Spark
        sc.close();
    }
}