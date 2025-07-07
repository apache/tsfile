#include "encoding/fire.h"

#include <gtest/gtest.h>

// -------- IntFire Unit Tests --------
TEST(IntFireTest, PredictWithoutTraining) {
    IntFire fire(2);  // learnShift = 2

    int input = 100;
    int output = fire.predict(input);

    EXPECT_EQ(output, 100);
}

TEST(IntFireTest, TrainAndPredictSimple) {
    IntFire fire(2);

    fire.train(100, 105, 5);  // delta = 5, accumulator += 5

    int result = fire.predict(105);
    EXPECT_EQ(result, 105);
}

TEST(IntFireTest, ResetWorks) {
    IntFire fire(2);
    fire.train(100, 110, 10);
    fire.reset();

    int result = fire.predict(200);
    EXPECT_EQ(result, 200);
}

// -------- LongFire Unit Tests --------
TEST(LongFireTest, PredictWithoutTraining) {
    LongFire fire(2);

    long long input = 1000LL;
    long long output = fire.predict(input);

    EXPECT_EQ(output, 1000LL);
}

TEST(LongFireTest, TrainAndPredictSimple) {
    LongFire fire(2);

    fire.train(1000, 1050, 50);  // delta = 50

    long long result = fire.predict(1050);
    EXPECT_EQ(result, 1050LL);
}

TEST(LongFireTest, MultipleTrainPredictSteps) {
    LongFire fire(2);

    fire.train(1000, 1100, 100);
    fire.train(1100, 1200, 100);
    fire.train(1200, 1300, 100);

    long long prediction = fire.predict(1300);
    EXPECT_GE(prediction, 1300);
}
