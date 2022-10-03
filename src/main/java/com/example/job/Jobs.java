package com.example.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;

@Path("/job")
public class Jobs {

    private static final Logger logger = LoggerFactory.getLogger(Jobs.class);

    private final KubernetesClient client;

    public Jobs(KubernetesClient client) {
        this.client = client;
    }

    @Path("/toupper_containers")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String runToUpperJob(@QueryParam("input") String input) {

        List<Character> chars = new ArrayList<>();
 
        for (char ch: input.toCharArray()) {
            chars.add(ch);
        }

        String jobName = "pi-" + UUID.randomUUID();

        try {

            // Create the job...
            JobBuilder jobBuilder = new JobBuilder()
                .withApiVersion("batch/v1")
                .withNewMetadata()
                .withName(jobName)
                .withLabels(Collections.singletonMap("label1", "maximum-length-of-63-characters"))
                .withAnnotations(Collections.singletonMap("annotation1", "some-very-long-annotation"))
                .endMetadata()
                .withNewSpec()
                .withNewTemplate()
                .withNewSpec()
                .withRestartPolicy("Never")
                .endSpec()
                .endTemplate()
                .endSpec();

            // ...and add a container per letter
            int containerCount = 0;

            for (Character character : chars) {
                containerCount++;

                jobBuilder.editSpec().editTemplate().editSpec().addNewContainer()
                .withName(jobName + "-" + containerCount)
                .withImage("perl")
                .withArgs("perl", "-wle", "print uc '" + character + "'")
                .withNewResources().addToLimits("memory", new Quantity("100Mi")).endResources()
                .endContainer()
                .endSpec().endTemplate().endSpec();
            }

            // Create the job
            Job job = jobBuilder.build();

            logger.info("Creating job pi.");
            client.batch().v1().jobs().createOrReplace(job);

            // Monitor the job progress
            client.batch().v1().jobs().withName(jobName)
                .waitUntilCondition(j -> j.getStatus().getActive() != null, 2, TimeUnit.MINUTES);

            logger.info("Job is active: " + jobName);
            
            client.batch().v1().jobs().withName(jobName)
                .waitUntilCondition(j -> j.getStatus().getActive() == null, 2, TimeUnit.MINUTES);

            logger.info("Job is finished: " + jobName);
          
            // Get All pods created by the job
            PodList podList = client.pods().withLabel("job-name", job.getMetadata().getName()).list();

            // Wait for pod to complete
            client.pods().withName(podList.getItems().get(0).getMetadata().getName())
                .waitUntilCondition(pod -> pod.getStatus().getPhase().equals("Succeeded"), 2, TimeUnit.MINUTES);

            // Collate job output
            StringBuffer output = new StringBuffer();
            
            for(int i = 1; i <= containerCount; i++) {
                output.append(client.pods().withName(podList.getItems().get(0).getMetadata().getName()).inContainer(jobName + "-" + i).getLog().trim());
            }

            logger.info(output.toString());

            return output.toString();

        } catch (KubernetesClientException e) {
            logger.error("Unable to create job", e);
        }

        return "";
    }


    @Path("/toupper_jobs")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String runToUpperJobs(@QueryParam("input") String input) {

        List<Character> chars = new ArrayList<>();
 
        for (char ch: input.toCharArray()) {
            chars.add(ch);
        }

        // Define the jobs
        List<Job> jobList = new ArrayList<>();

        try {
            for (Character character : chars) {

                String jobName = "pi-" + UUID.randomUUID();

                // Create the job...
                JobBuilder jobBuilder = new JobBuilder()
                    .withApiVersion("batch/v1")
                    .withNewMetadata()
                    .withName(jobName)
                    .withLabels(Collections.singletonMap("label1", "maximum-length-of-63-characters"))
                    .withAnnotations(Collections.singletonMap("annotation1", "some-very-long-annotation"))
                    .endMetadata()
                    .withNewSpec()
                    .withNewTemplate()
                    .withNewSpec()
                    .addNewContainer()
                    .withName(jobName + "-" + 1)
                    .withImage("ubi9")
                    .withArgs("/bin/bash", "-c", "C=" + character + "; echo ${C^}")
                    // .withImage("perl")
                    // .withArgs("perl", "-wle", "print uc '" + character + "'")
                    .withNewResources().addToLimits("memory", new Quantity("100Mi")).endResources()
                    .endContainer()
                    .withRestartPolicy("Never")
                    .endSpec()
                    .endTemplate()
                    .endSpec();

                // Create the job
                Job job = jobBuilder.build();

                jobList.add(job);
            }

            // Create the jobs
            for (Job job : jobList) {
                logger.info("Creating job " + job.getMetadata().getName());
                client.batch().v1().jobs().createOrReplace(job);                
            }

            // Monitor the jobs and collect the output
            StringBuffer output = new StringBuffer();

            for (Job job : jobList) {

                String jobName = job.getMetadata().getName();

                logger.info("Waiting for job to finish: " + jobName);

                // Monitor the job progress
                client.batch().v1().jobs().withName(jobName)
                    .waitUntilCondition(j -> j.getStatus().getCompletionTime() != null, 2, TimeUnit.MINUTES);

                logger.info("Job is finished: " + jobName);
            
                // Get All pods created by the job
                PodList podList = client.pods().withLabel("job-name", jobName).list();

                // Wait for pod to complete
                client.pods().withName(podList.getItems().get(0).getMetadata().getName())
                    .waitUntilCondition(pod -> pod.getStatus().getPhase().equals("Succeeded"), 2, TimeUnit.MINUTES);

                logger.info("Pod is finished: " + jobName);
            
                // Collate job output
                output.append(client.pods().withName(podList.getItems().get(0).getMetadata().getName()).inContainer(jobName + "-" + 1).getLog().trim());
            }

            logger.info(output.toString());

            return output.toString();

        } catch (KubernetesClientException e) {
            logger.error("Unable to create job", e);
        }

        return "";
    }
}