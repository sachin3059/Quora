package QuoraProject.repositories;


import QuoraProject.models.Question;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface QuestionRepository extends ReactiveMongoRepository<Question, String> {

}


/*
flux  is not a list , act as a producer
mono represent single value
 */
