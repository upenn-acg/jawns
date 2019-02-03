import jawns.Jawns;
import jawns.Job;
import jawns.LocalFile;
import jawns.RemoteFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SubmitTestJobs {

    public static void main(String[] args) {

        List<Job> myJobs = new ArrayList<>();

        myJobs.add(new Job()
                .withCommand("/bin/sleep 10")
                .withTimeoutSeconds(2)
                .withTag("sleep-timeout"));

        myJobs.add(new Job()
                .withCommand("/bin/echo hey stdout jawn")
                .withTag("stdout"));

        myJobs.add(new Job()
        .withCommand("/bin/echo hey stderr jawn >&2")
                .withTag("stderr"));

        myJobs.add(new Job()
                .withCommand("cat prefile.txt > postfile.txt")
                .withPreFile(new LocalFile("prefile.txt"),new RemoteFile("prefile.txt"))
                .withPostFile(new RemoteFile("postfile.txt"),new File("postfile.txt"))
                .withTag("pre-post-files"));

        myJobs.add(new Job()
                .withCommand("cat prefile.txt > postfile.txt")
                .withPreFile(new LocalFile("prefile.txt"),new RemoteFile("prefile.txt"))
                .withPostFile(new RemoteFile("nonexistent.txt"),new File("postfile.txt"))
                .withTag("error-post-file"));

        myJobs.add(new Job()
                .withCommand("ls foobar")
                .withTag("failing"));

        myJobs.add(new Job()
                .withCommand("/bin/sleep 180")
                .withTag("sleep-cancel"));

        Jawns J = new Jawns("test");
        boolean ok = J.submitJobs(myJobs);
        assert ok;

    }

}
