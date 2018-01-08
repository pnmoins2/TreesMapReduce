package cs.Lab2.TreeCountByType;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;


public class TreeCountByType extends Configured implements Tool {

        public int run(String[] args) throws Exception {
                if (args.length != 2) {

                    System.out.println("Usage: [input] [output]");

                    System.exit(-1);

                }

                // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

                Job job = Job.getInstance(getConf());

                job.setJobName("TreeCountByType");


                // On précise les classes MyProgram, Map et Reduce

                job.setJarByClass(TreeCountByType.class);

                job.setMapperClass(TreeCountByTypeMapper.class);

                job.setReducerClass(TreeCountByTypeReducer.class);


                // Définition des types clé/valeur de notre problème

                job.setMapOutputKeyClass(Text.class);

                job.setMapOutputValueClass(IntWritable.class);


                job.setOutputKeyClass(Text.class);

                job.setOutputValueClass(IntWritable.class);


                // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

                String commaSeparatedPaths = args[0];
                FileInputFormat.addInputPaths(job, commaSeparatedPaths);

                Path outputFilePath = new Path(args[1]);
                FileOutputFormat.setOutputPath(job, outputFilePath);


                //Suppression du fichier de sortie s'il existe déjà

                FileSystem fs = FileSystem.newInstance(getConf());

                if (fs.exists(outputFilePath)) {
                    fs.delete(outputFilePath, true);
                }


                return job.waitForCompletion(true) ? 0: 1;
        
        }


        public static void main(String[] args) throws Exception {

                TreeCountByType treeCountByTypeDriver = new TreeCountByType();

                int res = ToolRunner.run(treeCountByTypeDriver, args);

                System.exit(res);

        }

}
