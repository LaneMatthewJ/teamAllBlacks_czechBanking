import graphviz 
from sklearn.tree import plot_tree
from sklearn.model_selection import train_test_split, KFold
from sklearn import metrics
import numpy as np 
from matplotlib import pyplot as plt
import networkx as nx

def getTrainTestSplit(xValues, yValues): 
    X_train, X_test, y_train, y_test = train_test_split(xValues, yValues, test_size=0.10)

    return (X_train, X_test, y_train, y_test)



def doKfoldCrossValidation(X,y, model, isRegression):     
    kf = KFold(n_splits=5, random_state=None, shuffle=True)
    errorRates = []
    errorDifferences = []
    
    for fold, (train_index, test_index) in enumerate(kf.split(X, y)):
        X_train, X_test = X[train_index, :], X[test_index, :]
        y_train, y_test = y[train_index], y[test_index]
        mlModel = model()
        mlModel.fit(X_train, y_train)
        y_test_pred = mlModel.score(X_test, y_test)
        errorRates.append(y_test_pred)

        # Save off y_test_pred in a list or something -- you can average it all when done
        
        if isRegression: 
            errorDifferences.append( np.mean(np.abs(y_test.reshape((len(y_test),)) - y_test_pred) ))
        else: 
            errorDifferences.append( 0 )
        

    return ( np.mean(errorRates), np.mean(errorDifferences) )

def getAccuracy(X,y, model, isRegression): 
    xTrain, xTest, yTrain, yTest = getTrainTestSplit(X,y)
    modelObject = model()
    modelObject.fit(xTrain, yTrain)
    yPredicted = modelObject.predict(xTest)
    
    if isRegression: 
        return (modelObject, metrics.mean_squared_error(yTest, yPredicted))
    else:
        return (modelObject, metrics.accuracy_score(yTest,yPredicted))



def printImportance(model, X): 
    # Prints the feature importance of a 
    importances = model.feature_importances_
    std = np.std([model.feature_importances_ for tree in model.estimators_],
                 axis=0)
    indices = np.argsort(importances)[::-1]

    # Print the feature ranking
    print("Feature ranking:")
    for f in range(X.shape[1]):
        print("%d. feature %d (%f)" % (f + 1, indices[f], importances[indices[f]]))

    # Plot the impurity-based feature importances of the forest
    plt.figure()
    plt.title("Feature importances")
    plt.bar(range(X.shape[1]), importances[indices],
            color="r", yerr=std[indices], align="center")
    plt.xticks(range(X.shape[1]), indices)
    plt.xlim([-1, X.shape[1]])
    plt.show()



def applyModelToDataframe(df, xColumnsArray, yVectorName, model, isRegression, classifierType='linear'): 

    X = df[xColumnsArray].values
    y = df[yVectorName].values.reshape((len(df[yVectorName],)))

    (kFoldAccuracy, kfoldDifferences ) = doKfoldCrossValidation(X,y, model, isRegression)
    (modelObject, accuracy) = getAccuracy(X,y, model, isRegression)
    
    if classifierType=='randomForest':
        printImportance(modelObject, X)
    elif classifierType == 'decisionTree': 
        plt.figure(figsize=(15,15)) 
        plot_tree(modelObject, fontsize=12)
        plt.show()
    
    return { "kFoldAccuracyScore": kFoldAccuracy, "MeanSquareError": accuracy, "meanOfDifferencesInPrediction": kfoldDifferences } if isRegression else {
        "kFoldAccuracyScore": kFoldAccuracy, "classificationAcc": accuracy, "meanOfDifferencesInPrediction": kfoldDifferences }


def generateCorrelationNetwork(df, threshold=0.5): 
    # Calculate the correlation between individuals. We have to transpose first, because the corr function calculate the pairwise correlations between columns.
    corr = df.corr()
    corr
    # Transform it in a links data frame (3 columns only):
    links = corr.stack().reset_index()
    links.columns = ['var1', 'var2','value']
    links

    # Keep only correlation over a threshold and remove self correlation (cor(A,A)=1)
    links_filteredNegative=links.loc[ (links['value'] <  -threshold) & (links['var1'] != links['var2']) ]
    links_filteredPos=links.loc[ (links['value'] > threshold) & (links['var1'] != links['var2']) ]

    # Build your graph
    Gneg=nx.from_pandas_edgelist(links_filteredNegative, 'var1', 'var2')
    Gpos=nx.from_pandas_edgelist(links_filteredPos, 'var1', 'var2')

    kVal = 0.35
    gNegPos = nx.spring_layout(Gneg, k=kVal, iterations=20)
    gPosPos = nx.spring_layout(Gpos, k=kVal, iterations=20)
    # Plot the network:
    plt.figure(figsize=(7,7))
    nx.draw(Gneg,gNegPos, with_labels=True, node_color='lightcoral', node_size=1000, edge_color='black', linewidths=1, font_size=10)
    nx.draw(Gpos,gPosPos, with_labels=True, node_color='cornflowerblue', node_size=1000, edge_color='black', linewidths=1, font_size=10)
    plt.show()